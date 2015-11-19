// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cznic/b"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/regioninfo"
	"github.com/tsuna/gohbase/zk"
	"golang.org/x/net/context"
)

// Constants
var (
	// Name of the meta region.
	metaTableName = []byte("hbase:meta")

	infoFamily = map[string][]string{
		"info": nil,
	}

	// ErrDeadline is returned when the deadline of a request has been exceeded
	ErrDeadline = errors.New("deadline exceeded")

	// TableNotFound is returned when attempting to access a table that
	// doesn't exist on this cluster.
	TableNotFound = errors.New("table not found")

	// Default timeouts

	// How long to wait for a region lookup (either meta lookup or finding
	// meta in ZooKeeper).  Should be greater than or equal to the ZooKeeper
	// session timeout.
	regionLookupTimeout = 30 * time.Second

	backoffStart = 16 * time.Millisecond
)

const (
	StandardClient = iota
	AdminClient
)

type Option func(*Client)

type newRegResult struct {
	Client *region.Client
	Err    error
}

// region -> client cache.
type regionClientCache struct {
	m sync.Mutex

	clients map[*regioninfo.Info]*region.Client

	// Used to quickly look up all the regioninfos that map to a specific client
	clientsToInfos map[*region.Client][]*regioninfo.Info
}

func (rcc *regionClientCache) get(r *regioninfo.Info) *region.Client {
	rcc.m.Lock()
	c := rcc.clients[r]
	rcc.m.Unlock()
	return c
}

func (rcc *regionClientCache) put(r *regioninfo.Info, c *region.Client) {
	rcc.m.Lock()
	rcc.clients[r] = c
	lst := rcc.clientsToInfos[c]
	rcc.clientsToInfos[c] = append(lst, r)
	rcc.m.Unlock()
}

func (rcc *regionClientCache) del(r *regioninfo.Info) {
	rcc.m.Lock()
	c := rcc.clients[r]

	if c != nil {
		// c can be nil if the regioninfo is not in the cache
		// e.g. it's already been deleted.
		delete(rcc.clients, r)

		var index int
		for i, reg := range rcc.clientsToInfos[c] {
			if reg == r {
				index = i
			}
		}
		rcc.clientsToInfos[c] = append(
			rcc.clientsToInfos[c][:index],
			rcc.clientsToInfos[c][index+1:]...)
	}
	rcc.m.Unlock()
}

func (rcc *regionClientCache) clientDown(reg *regioninfo.Info) []*regioninfo.Info {
	rcc.m.Lock()
	var downregions []*regioninfo.Info
	c := rcc.clients[reg]
	for _, sharedReg := range rcc.clientsToInfos[c] {
		succ := sharedReg.MarkUnavailable()
		delete(rcc.clients, sharedReg)
		if succ {
			downregions = append(downregions, sharedReg)
		}
	}
	delete(rcc.clientsToInfos, c)
	rcc.m.Unlock()
	return downregions
}

func (rcc *regionClientCache) checkForClient(host string, port uint16) *region.Client {
	rcc.m.Lock()
	for client := range rcc.clientsToInfos {
		if client.Host() == host && client.Port() == port {
			rcc.m.Unlock()
			return client
		}
	}
	rcc.m.Unlock()
	return nil
}

// key -> region cache.
type keyRegionCache struct {
	m sync.Mutex

	// Maps a []byte of a region start key to a *regioninfo.Info
	regions *b.Tree
}

func (krc *keyRegionCache) get(key []byte) ([]byte, *regioninfo.Info) {
	// When seeking - "The Enumerator's position is possibly after the last item in the tree"
	// http://godoc.org/github.com/cznic/b#Tree.Set
	krc.m.Lock()
	enum, ok := krc.regions.Seek(key)
	k, v, err := enum.Prev()
	if err == io.EOF && krc.regions.Len() > 0 {
		// We're past the end of the tree. Return the last element instead.
		// (Without this code we always get a cache miss and create a new client for each req.)
		k, v = krc.regions.Last()
		err = nil
	} else if !ok {
		k, v, err = enum.Prev()
	}
	// TODO: It would be nice if we could do just enum.Get() to avoid the
	// unnecessary cost of seeking to the next entry.
	krc.m.Unlock()
	if err != nil {
		return nil, nil
	}
	return k.([]byte), v.(*regioninfo.Info)
}

func (krc *keyRegionCache) put(key []byte, reg *regioninfo.Info) *regioninfo.Info {
	krc.m.Lock()
	// TODO: We need to remove all the entries that are overlap with the range
	// of the new region being added here, if any.
	oldV, _ := krc.regions.Put(key, func(interface{}, bool) (interface{}, bool) {
		return reg, true
	})
	krc.m.Unlock()
	if oldV == nil {
		return nil
	}
	return oldV.(*regioninfo.Info)
}

func (krc *keyRegionCache) del(key []byte) bool {
	krc.m.Lock()
	success := krc.regions.Delete(key)
	krc.m.Unlock()
	return success
}

// A Client provides access to an HBase cluster.
type Client struct {
	clientType int

	zkquorum string

	regions keyRegionCache

	// TODO: document what this protects.
	regionsLock sync.Mutex

	// Maps a *regioninfo.Info to the *region.Client that we think currently
	// serves it.
	clients regionClientCache

	metaRegionInfo *regioninfo.Info
	metaClient     *region.Client

	adminRegionInfo *regioninfo.Info
	adminClient     *region.Client

	// The maximum size of the RPC queue in the region client
	rpcQueueSize int

	// The timeout before flushing the RPC queue in the region client
	flushInterval time.Duration
	
	// Master and Meta resource names
	master zk.ResourceName
	meta   zk.ResourceName
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string, options ...Option) *Client {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new client.")
	c := &Client{
		clientType: StandardClient,
		regions:    keyRegionCache{regions: b.TreeNew(regioninfo.CompareGeneric)},
		clients: regionClientCache{
			clients:        make(map[*regioninfo.Info]*region.Client),
			clientsToInfos: make(map[*region.Client][]*regioninfo.Info),
		},
		zkquorum:      zkquorum,
		rpcQueueSize:  100,
		flushInterval: 20 * time.Millisecond,
		metaRegionInfo: &regioninfo.Info{
			Table:      []byte("hbase:meta"),
			RegionName: []byte("hbase:meta,,1"),
			StopKey:    []byte{},
		},
		adminRegionInfo: &regioninfo.Info{},
	}
	for _, option := range options {
		option(c)
	}
	if c.master == (zk.ResourceName)("") {
		c.master = zk.ResourceName("/hbase/master")
	}
	if c.meta == (zk.ResourceName)("") {
		c.meta = zk.ResourceName("/hbase/meta-region-server")
	}
	return c
}

// RpcQueueSize will return an option that will set the size of the RPC queues
// used in a given client
func RpcQueueSize(size int) Option {
	return func(c *Client) {
		c.rpcQueueSize = size
	}
}

// FlushInterval will return an option that will set the timeout for flushing
// the RPC queues used in a given client
func FlushInterval(interval time.Duration) Option {
	return func(c *Client) {
		c.flushInterval = interval
	}
}

func Admin() Option {
	return func(c *Client) {
		c.clientType = AdminClient
	}
}

// Options for specifying master resource name (e.g. "/hbase/master")
func SetMaster(master zk.ResourceName) Option {
	return func(c *Client) {
		c.master = master
	}
}

// Options for specifying meta resource name (e.g. "/hbase/meta-region-server")
func SetMeta(meta zk.ResourceName) Option {
	return func(c *Client) {
		c.meta = meta
	}
}

// CheckTable returns an error if the given table name doesn't exist.
func (c *Client) CheckTable(ctx context.Context, table string) error {
	getStr, err := hrpc.NewGetStr(ctx, table, "theKey")
	if err == nil {
		_, err = c.SendRPC(getStr)
	}
	return err
}

// Scan retrieves the values specified in families from the given range.
func (c *Client) Scan(s *hrpc.Scan) ([]*hrpc.Result, error) {
	var results []*pb.Result
	var scanres *pb.ScanResponse
	var rpc *hrpc.Scan
	ctx := s.GetContext()
	table := s.Table()
	families := s.GetFamilies()
	filters := s.GetFilter()
	startRow := s.GetStartRow()
	stopRow := s.GetStopRow()
	for {
		// Make a new Scan RPC for this region
		if rpc != nil {
			// If it's not the first region, we want to start at whatever the
			// last region's StopKey was
			startRow = rpc.GetRegionStop()
		}

		rpc, err := hrpc.NewScanRange(ctx, table, startRow, stopRow,
			hrpc.Families(families), hrpc.Filters(filters))
		if err != nil {
			return nil, err
		}

		res, err := c.sendRPC(rpc)
		if err != nil {
			return nil, err
		}
		scanres = res.(*pb.ScanResponse)
		results = append(results, scanres.Results...)

		// TODO: The more_results field of the ScanResponse object was always
		// true, so we should figure out if there's a better way to know when
		// to move on to the next region than making an extra request and
		// seeing if there were no results
		for len(scanres.Results) != 0 {
			rpc = hrpc.NewScanFromID(ctx, table, *scanres.ScannerId, rpc.Key())

			res, err = c.sendRPC(rpc)
			if err != nil {
				return nil, err
			}
			scanres = res.(*pb.ScanResponse)
			results = append(results, scanres.Results...)
		}

		rpc = hrpc.NewCloseFromID(ctx, table, *scanres.ScannerId, rpc.Key())
		if err != nil {
			return nil, err
		}
		res, err = c.sendRPC(rpc)

		// Check to see if this region is the last we should scan (either
		// because (1) it's the last region or (3) because its stop_key is
		// greater than or equal to the stop_key of this scanner provided
		// that (2) we're not trying to scan until the end of the table).
		// (1)
		if len(rpc.GetRegionStop()) == 0 ||
			// (2)                (3)
			len(stopRow) != 0 && bytes.Compare(stopRow, rpc.GetRegionStop()) <= 0 {
			// Do we want to be returning a slice of Result objects or should we just
			// put all the Cells into the same Result object?
			localResults := make([]*hrpc.Result, len(results))
			for idx, result := range results {
				localResults[idx] = hrpc.ToLocalResult(result)
			}
			return localResults, nil
		}
	}
}

func (c *Client) SendRPC(rpc hrpc.Call) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(rpc)

	var rsp *hrpc.Result
	switch r := pbmsg.(type) {
	case *pb.GetResponse:
		rsp = hrpc.ToLocalResult(r.Result)
	case *pb.MutateResponse:
		rsp = hrpc.ToLocalResult(r.Result)
	}

	return rsp, err
}

func (c *Client) sendRPC(rpc hrpc.Call) (proto.Message, error) {
	// Check the cache for a region that can handle this request
	reg := c.getRegionFromCache(rpc.Table(), rpc.Key())
	if reg != nil {
		return c.sendRPCToRegion(rpc, reg)
	} else {
		return c.findRegionForRPC(rpc)
	}
}

func (c *Client) sendRPCToRegion(rpc hrpc.Call, reg *regioninfo.Info) (proto.Message, error) {
	// On the first sendRPC to the meta or admin regions, a goroutine must be
	// manually kicked off for the meta or admin region client
	if c.adminClient == nil && reg == c.adminRegionInfo && !c.adminRegionInfo.IsUnavailable() ||
		c.metaClient == nil && reg == c.metaRegionInfo && !c.metaRegionInfo.IsUnavailable() {
		c.regionsLock.Lock()
		if reg == c.metaRegionInfo && !c.metaRegionInfo.IsUnavailable() ||
			reg == c.adminRegionInfo && !c.adminRegionInfo.IsUnavailable() {
			reg.MarkUnavailable()
			go c.reestablishRegion(reg)
		}
		c.regionsLock.Unlock()
	}
	// The region was in the cache, check
	// if the region is marked as available
	if !reg.IsUnavailable() {
		// The region is available

		rpc.SetRegion(reg)

		// Queue the RPC to be sent to the region
		client := c.clientFor(reg)
		var err error
		if client == nil {
			err = errors.New("no client for this region")
		} else {
			err = client.QueueRPC(rpc)
		}

		if err != nil {
			// There was an error queueing the RPC.
			// Mark the region as unavailable.
			first := reg.MarkUnavailable()
			// If this was the first goroutine to mark the region as
			// unavailable, start a goroutine to reestablish a connection
			if first {
				go c.reestablishRegion(reg)
			}
			// Block until the region becomes available.
			return c.waitOnRegion(rpc, reg)
		}

		// Wait for the response
		var res hrpc.RPCResult
		select {
		case res = <-rpc.GetResultChan():
		case <-rpc.GetContext().Done():
			return nil, ErrDeadline
		}

		// Check for errors
		if _, ok := res.Error.(region.RetryableError); ok {
			// There's an error specific to this region, but
			// our region client is fine. Mark this region as
			// unavailable (as opposed to all regions sharing
			// the client), and start a goroutine to reestablish
			// it.
			first := reg.MarkUnavailable()
			if first {
				go c.reestablishRegion(reg)
			}
			if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
				// The client won't be in the cache if this is the
				// meta or admin region
				c.clients.del(reg)
			}
			return c.waitOnRegion(rpc, reg)
		} else if _, ok := res.Error.(region.UnrecoverableError); ok {
			// If it was an unrecoverable error, the region client is
			// considered dead.
			if reg == c.metaRegionInfo || reg == c.adminRegionInfo {
				// If this is the admin client or the meta table, mark the
				// region as unavailable and start up a goroutine to
				// reconnect if it wasn't already marked as such.
				first := reg.MarkUnavailable()
				if first {
					go c.reestablishRegion(reg)
				}
			} else {
				// Else this is a normal region. Mark all the regions
				// sharing this region's client as unavailable, and start
				// a goroutine to reconnect for each of them.
				downregions := c.clients.clientDown(reg)
				for _, downreg := range downregions {
					go c.reestablishRegion(downreg)
				}
			}

			// Fall through to the case of the region being unavailable,
			// which will result in blocking until it's available again.
			return c.waitOnRegion(rpc, reg)
		} else {
			// RPC was successfully sent, or an unknown type of error
			// occurred. In either case, return the results.
			return res.Msg, res.Error
		}
	}
	return c.waitOnRegion(rpc, reg)
}

func (c *Client) waitOnRegion(rpc hrpc.Call, reg *regioninfo.Info) (proto.Message, error) {
	ch := reg.GetAvailabilityChan()
	if ch == nil {
		// WTF, this region is available? Maybe it was marked as such
		// since waitOnRegion was called.
		return c.sendRPC(rpc)
	}
	// The region is unavailable. Wait for it to become available,
	// or for the deadline to be exceeded.
	select {
	case <-ch:
		return c.sendRPC(rpc)
	case <-rpc.GetContext().Done():
		return nil, ErrDeadline
	}
}

func (c *Client) findRegionForRPC(rpc hrpc.Call) (proto.Message, error) {
	// The region was not in the cache, it
	// must be looked up in the meta table

	backoff := backoffStart
	ctx := rpc.GetContext()
	for {
		// Look up the region in the meta table
		reg, host, port, err := c.locateRegion(ctx, rpc.Table(), rpc.Key())

		if err != nil {
			if err == TableNotFound {
				return nil, err
			}
			// There was an error with the meta table. Let's sleep for some
			// backoff amount and retry.
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				return nil, err
			}
			continue
		}

		// Check that the region wasn't added to
		// the cache while we were looking it up.
		c.regionsLock.Lock()

		if existing := c.getRegionFromCache(rpc.Table(), rpc.Key()); existing != nil {
			// The region was added to the cache while we were looking it
			// up. Send the RPC to the region that was in the cache.
			c.regionsLock.Unlock()
			return c.sendRPCToRegion(rpc, existing)
		}

		// The region wasn't added to the cache while we were looking it
		// up. Mark this one as unavailable and add it to the cache.
		reg.MarkUnavailable()
		c.regions.put(reg.RegionName, reg)

		c.regionsLock.Unlock()

		// Start a goroutine to connect to the region
		go c.establishRegion(reg, host, port)

		// Wait for the new region to become
		// available, and then send the RPC
		return c.waitOnRegion(rpc, reg)
	}
}

// Searches in the regions cache for the region hosting the given row.
func (c *Client) getRegionFromCache(table, key []byte) *regioninfo.Info {
	if c.clientType == AdminClient {
		return c.adminRegionInfo
	} else if bytes.Equal(table, metaTableName) {
		return c.metaRegionInfo
	}
	regionName := createRegionSearchKey(table, key)
	regionKey, region := c.regions.get(regionName)
	if region == nil || !isCacheKeyForTable(table, regionKey) {
		return nil
	}

	if len(region.StopKey) != 0 &&
		// If the stop key is an empty byte array, it means this region is the
		// last region for this table and this key ought to be in that region.
		bytes.Compare(key, region.StopKey) >= 0 {
		return nil
	}

	return region
}

// Checks whether or not the given cache key is for the given table.
func isCacheKeyForTable(table, cacheKey []byte) bool {
	// Check we found an entry that's really for the requested table.
	for i := 0; i < len(table); i++ {
		if table[i] != cacheKey[i] { // This table isn't in the map, we found
			return false // a key which is for another table.
		}
	}

	// Make sure we didn't find another key that's for another table
	// whose name is a prefix of the table name we were given.
	return cacheKey[len(table)] == ','
}

// Creates the META key to search for in order to locate the given key.
func createRegionSearchKey(table, key []byte) []byte {
	metaKey := make([]byte, 0, len(table)+len(key)+3)
	metaKey = append(metaKey, table...)
	metaKey = append(metaKey, ',')
	metaKey = append(metaKey, key...)
	metaKey = append(metaKey, ',')
	// ':' is the first byte greater than '9'.  We always want to find the
	// entry with the greatest timestamp, so by looking right before ':'
	// we'll find it.
	metaKey = append(metaKey, ':')
	return metaKey
}

// Returns the client currently known to hose the given region, or NULL.
func (c *Client) clientFor(region *regioninfo.Info) *region.Client {
	if c.clientType == AdminClient {
		return c.adminClient
	}
	if region == c.metaRegionInfo {
		return c.metaClient
	}
	return c.clients.get(region)
}

// Locates the region in which the given row key for the given table is.
func (c *Client) locateRegion(ctx context.Context,
	table, key []byte) (*regioninfo.Info, string, uint16, error) {

	metaKey := createRegionSearchKey(table, key)
	rpc, err := hrpc.NewGetBefore(ctx, metaTableName, metaKey, hrpc.Families(infoFamily))
	if err != nil {
		return nil, "", 0, err
	}
	rpc.SetRegion(c.metaRegionInfo)
	resp, err := c.sendRPC(rpc)

	if err != nil {
		ch := c.metaRegionInfo.GetAvailabilityChan()
		if ch != nil {
			select {
			case <-ch:
				return c.locateRegion(ctx, table, key)
			case <-rpc.GetContext().Done():
				return nil, "", 0, ErrDeadline
			}
		} else {
			return nil, "", 0, err
		}
	}

	metaRow := resp.(*pb.GetResponse)
	if metaRow.Result == nil {
		return nil, "", 0, TableNotFound
	}

	reg, host, port, err := c.parseMetaTableResponse(metaRow)
	if err != nil {
		return nil, "", 0, err
	}
	if !bytes.Equal(table, reg.Table) {
		// This would indicate a bug in HBase.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong table!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	} else if len(reg.StopKey) != 0 &&
		bytes.Compare(key, reg.StopKey) >= 0 {
		// This would indicate a hole in the meta table.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong region!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	}
	return reg, host, port, nil
}

// parseMetaTableResponse parses the contents of a row from the meta table.
// It's guaranteed to return a region info and a host/port OR return an error.
func (c *Client) parseMetaTableResponse(metaRow *pb.GetResponse) (
	*regioninfo.Info, string, uint16, error) {

	var reg *regioninfo.Info
	var host string
	var port uint16

	for _, cell := range metaRow.Result.Cell {
		switch string(cell.Qualifier) {
		case "regioninfo":
			var err error
			reg, err = regioninfo.InfoFromCell(cell)
			if err != nil {
				return nil, "", 0, err
			}
		case "server":
			value := cell.Value
			if len(value) == 0 {
				continue // Empty during NSRE.
			}
			colon := bytes.IndexByte(value, ':')
			if colon < 1 { // Colon can't be at the beginning.
				return nil, "", 0,
					fmt.Errorf("broken meta: no colon found in info:server %q", cell)
			}
			host = string(value[:colon])
			portU64, err := strconv.ParseUint(string(value[colon+1:]), 10, 16)
			if err != nil {
				return nil, "", 0, err
			}
			port = uint16(portU64)
		default:
			// Other kinds of qualifiers: ignore them.
			// TODO: If this is the parent of a split region, there are two other
			// KVs that could be useful: `info:splitA' and `info:splitB'.
			// Need to investigate whether we can use those as a hint to update our
			// regions_cache with the daughter regions of the split.
		}
	}

	if reg == nil {
		// There was no regioninfo in the row in meta, this is really not
		// expected.
		err := fmt.Errorf("Meta seems to be broken, there was no regioninfo in %s",
			metaRow)
		log.Error(err.Error())
		return nil, "", 0, err
	} else if port == 0 { // Either both `host' and `port' are set, or both aren't.
		return nil, "", 0, fmt.Errorf("Meta doesn't have a server location in %s",
			metaRow)
	}

	return reg, host, port, nil
}

func (c *Client) reestablishRegion(reg *regioninfo.Info) {
	c.establishRegion(reg, "", 0)
}

func (c *Client) establishRegion(originalReg *regioninfo.Info, host string, port uint16) {
	var err error
	reg := originalReg
	backoff := backoffStart

	for {
		ctx, _ := context.WithTimeout(context.Background(), regionLookupTimeout)
		if port != 0 && err == nil {
			// If this isn't the admin or meta region, check if a client
			// for this host/port already exists
			if c.clientType != AdminClient && reg != c.metaRegionInfo {
				client := c.clients.checkForClient(host, port)
				if client != nil {
					// There's already a client, add it to the
					// cache and mark the new region as available.
					c.clients.put(reg, client)
					originalReg.MarkAvailable()
					return
				}
			}
			// Make this channel buffered so that if we time out we don't
			// block the newRegion goroutine forever.
			ch := make(chan newRegResult, 1)
			var clientType region.ClientType
			if c.clientType == StandardClient {
				clientType = region.RegionClient
			} else {
				clientType = region.MasterClient
			}
			go newRegion(ctx, ch, clientType, host, port, c.rpcQueueSize, c.flushInterval)

			select {
			case res := <-ch:
				if res.Err == nil {
					if c.clientType == AdminClient {
						c.adminClient = res.Client
					} else if reg == c.metaRegionInfo {
						c.metaClient = res.Client
					} else {
						c.clients.put(reg, res.Client)
						if reg != originalReg {
							// Here `reg' is guaranteed to be available, so we
							// must publish the region->client mapping first,
							// because as soon as we add it to the key->region
							// mapping here, concurrent readers are gonna want
							// to find the client.
							c.regions.put(reg.RegionName, reg)
						}
					}
					originalReg.MarkAvailable()
					return
				} else {
					err = res.Err
				}
			case <-ctx.Done():
				err = ErrDeadline
			}
		}
		if err != nil {
			if err == TableNotFound {
				c.regions.del(originalReg.RegionName)
				originalReg.MarkAvailable()
				return
			}
			// This will be hit if either there was an error locating the
			// region, or the region was located but there was an error
			// connecting to it.
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				continue
			}
		}
		if c.clientType == AdminClient {
			host, port, err = c.zkLookup(ctx, c.master)
		} else if reg == c.metaRegionInfo {
			host, port, err = c.zkLookup(ctx, c.meta)
		} else {
			reg, host, port, err = c.locateRegion(ctx, originalReg.Table, originalReg.StartKey)
		}
	}
}

func sleepAndIncreaseBackoff(ctx context.Context, backoff time.Duration) (time.Duration, error) {
	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return 0, ErrDeadline
	}
	// TODO: Revisit how we back off here.
	if backoff < 5000*time.Millisecond {
		return backoff * 2, nil
	} else {
		return backoff + 5000*time.Millisecond, nil
	}
}

func newRegion(ctx context.Context, ret chan newRegResult, clientType region.ClientType,
	host string, port uint16, queueSize int, queueTimeout time.Duration) {
	c, e := region.NewClient(host, port, clientType, queueSize, queueTimeout)
	select {
	case ret <- newRegResult{c, e}:
		// Hooray!
	case <-ctx.Done():
		// We timed out, too bad, nobody expects this client anymore, ditch it.
		c.Close()
	}
}

// zkResult contains the result of a ZooKeeper lookup (when we're looking for
// the meta region or the HMaster).
type zkResult struct {
	host string
	port uint16
	err  error
}

// Asynchronously looks up the meta region or HMaster in ZooKeeper.
func (c *Client) zkLookup(ctx context.Context, res zk.ResourceName) (string, uint16, error) {
	// We make this a buffered channel so that if we stop waiting due to a
	// timeout, we won't block the zkLookupSync() that we start in a
	// separate goroutine.
	reschan := make(chan zkResult, 1)
	go c.zkLookupSync(res, reschan)
	select {
	case res := <-reschan:
		return res.host, res.port, res.err
	case <-ctx.Done():
		return "", 0, ErrDeadline
	}
}

// Synchronously looks up the meta region or HMaster in ZooKeeper.
func (c *Client) zkLookupSync(res zk.ResourceName, reschan chan<- zkResult) {
	host, port, err := zk.LocateResource(c.zkquorum, res)
	// This is guaranteed to never block as the channel is always buffered.
	reschan <- zkResult{host, port, err}
}
