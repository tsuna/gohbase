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
)

type Option func(*Client)

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
	lst, ok := rcc.clientsToInfos[c]
	if ok {
		rcc.clientsToInfos[c] = append(lst, r)
	} else {
		rcc.clientsToInfos[c] = []*regioninfo.Info{r}
	}
	rcc.m.Unlock()
}

func (rcc *regionClientCache) delAll(c *region.Client) {
	rcc.m.Lock()
	for _, reg := range rcc.clientsToInfos[c] {
		delete(rcc.clients, reg)
	}
	delete(rcc.clientsToInfos, c)
	rcc.m.Unlock()
}

func (rcc *regionClientCache) getInfos(c *region.Client) []*regioninfo.Info {
	rcc.m.Lock()
	is := rcc.clientsToInfos[c]
	rcc.m.Unlock()
	return is
}

func (rcc *regionClientCache) checkForClient(host string, port uint16) *region.Client {
	rcc.m.Lock()
	for client, _ := range rcc.clientsToInfos {
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
	oldV, _ := krc.regions.Put(key, func(interface{}, bool) (interface{}, bool) { return reg, true })
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
	regions keyRegionCache

	// Maps a *regioninfo.Info to the *region.Client that we think currently
	// serves it.
	clients regionClientCache

	// Client connected to the RegionServer hosting the hbase:meta table.
	metaClient *region.Client

	zkquorum string

	// The maximum size of the RPC queue in the region client
	rpcQueueSize int

	// The timeout before flushing the RPC queue in the region client
	flushInterval time.Duration

	metaRegionInfo *regioninfo.Info

	regionDownLock sync.Mutex
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string, options ...Option) *Client {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new client.")
	c := &Client{
		regions: keyRegionCache{regions: b.TreeNew(regioninfo.CompareGeneric)},
		clients: regionClientCache{
			clients:        make(map[*regioninfo.Info]*region.Client),
			clientsToInfos: make(map[*region.Client][]*regioninfo.Info),
		},
		zkquorum:      zkquorum,
		rpcQueueSize:  100,
		flushInterval: 20,
		metaRegionInfo: &regioninfo.Info{
			Table:      []byte("hbase:meta"),
			RegionName: []byte("hbase:meta,,1"),
			StopKey:    []byte{},
		},
		regionDownLock: sync.Mutex{},
	}
	for _, option := range options {
		option(c)
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

// CheckTable returns an error if the given table name doesn't exist.
func (c *Client) CheckTable(ctx context.Context, table string) (*pb.GetResponse, error) {
	resp, err := c.sendRPC(hrpc.NewGetStr(ctx, table, "theKey", nil))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetResponse), err
}

// Get returns a single row fetched from HBase.
func (c *Client) Get(ctx context.Context, table, rowkey string, families map[string][]string) (*pb.GetResponse, error) {
	resp, err := c.sendRPC(hrpc.NewGetStr(ctx, table, rowkey, families))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetResponse), err
}

// Scan retrieves the values specified in families from the given range.
func (c *Client) Scan(ctx context.Context, table string, families map[string][]string, startRow, stopRow []byte) ([]*pb.Result, error) {
	var results []*pb.Result
	var scanres *pb.ScanResponse
	var rpc *hrpc.Scan
	for {
		// Make a new Scan RPC for this region
		if rpc == nil {
			// If it's the first region, just begin at the given startRow
			rpc = hrpc.NewScanStr(ctx, table, families, startRow, stopRow)
		} else {
			// If it's not the first region, we want to start at whatever the
			// last region's StopKey was
			rpc = hrpc.NewScanStr(ctx, table, families, rpc.GetRegionStop(), stopRow)
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
		// (1)                               (2)                  (3)
		if len(rpc.GetRegionStop()) == 0 || (len(stopRow) != 0 && bytes.Compare(stopRow, rpc.GetRegionStop()) <= 0) {
			return results, nil
		}
	}
}

// Put inserts or updates the values into the given row of the table.
func (c *Client) Put(ctx context.Context, table string, rowkey string, values map[string]map[string][]byte) (*pb.MutateResponse, error) {
	resp, err := c.sendRPC(hrpc.NewPutStr(ctx, table, rowkey, values))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.MutateResponse), err
}

// Delete removes values from the given row of the table.
func (c *Client) Delete(ctx context.Context, table, rowkey string, values map[string]map[string][]byte) (*pb.MutateResponse, error) {
	resp, err := c.sendRPC(hrpc.NewDelStr(ctx, table, rowkey, values))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.MutateResponse), err
}

// Append atomically appends all the given values to their current values in HBase.
func (c *Client) Append(ctx context.Context, table, rowkey string, values map[string]map[string][]byte) (*pb.MutateResponse, error) {
	resp, err := c.sendRPC(hrpc.NewAppStr(ctx, table, rowkey, values))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.MutateResponse), err
}

// Increment atomically increments the given values in HBase.
func (c *Client) Increment(ctx context.Context, table, rowkey string, values map[string]map[string][]byte) (*pb.MutateResponse, error) {
	resp, err := c.sendRPC(hrpc.NewIncStr(ctx, table, rowkey, values))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.MutateResponse), err
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

// Searches in the regions cache for the region hosting the given row.
func (c *Client) getRegion(table, key []byte) *regioninfo.Info {
	if bytes.Equal(table, metaTableName) {
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

// Returns the client currently known to hose the given region, or NULL.
func (c *Client) clientFor(region *regioninfo.Info) *region.Client {
	if region == c.metaRegionInfo {
		return c.metaClient
	}
	return c.clients.get(region)
}

// Queues an RPC targeted at a particular region for handling by the appropriate
// region client. Results will be written to the rpc's result and error
// channels.
func (c *Client) queueRPC(rpc hrpc.Call) error {
	table := rpc.Table()
	key := rpc.Key()
	reg := c.getRegion(table, key)

	// The first time an RPC is sent to the meta region, the meta client will
	// have not yet been intialized. Check if this is the case, try to mark
	// the meta region info as unavailable, and if it hadn't been marked as
	// unavailable yet start a goroutine to connect to it.
	if reg == c.metaRegionInfo && c.metaClient == nil {
		marked := c.metaRegionInfo.MarkUnavailable()
		if marked {
			go c.reestablishRegion(c.metaRegionInfo)
		}
	}

	var client *region.Client
	if reg != nil {
		ch := reg.GetAvailabilityChan()
		if ch != nil {
			select {
			case <-ch:
				return c.queueRPC(rpc)
			case <-rpc.Context().Done():
				return ErrDeadline
			}
		}

		client = c.clientFor(reg)
	} else {
		var err error
		client, reg, err = c.locateRegion(rpc.Context(), table, key)
		if err != nil {
			return err
		}
	}
	rpc.SetRegion(reg)
	return client.QueueRPC(rpc)
}

// sendRPC takes an RPC call, and will send it to the correct region server. If
// the correct region server is offline or otherwise unavailable, sendRPC will
// continually retry until the deadline set on the RPC's context is exceeded.
func (c *Client) sendRPC(rpc hrpc.Call) (proto.Message, error) {
	log.WithFields(log.Fields{
		"Type":  rpc.Name(),
		"Table": rpc.Table(),
		"Key":   rpc.Key(),
	}).Debug("New Request")
	err := c.queueRPC(rpc)
	if err == ErrDeadline {
		return nil, err
	}
	if err == nil {
		var res hrpc.RPCResult
		resch := rpc.GetResultChan()

		select {
		case res = <-resch:
		case <-rpc.Context().Done():
			return nil, ErrDeadline
		}

		err := res.Error
		if _, ok := err.(region.RetryableError); ok {
			return c.sendRPC(rpc)
		} else if _, ok := err.(region.UnrecoverableError); ok {
			// Prevents dropping into the else block below,
			// error handling happens a few lines down
		} else {
			return res.Msg, res.Error
		}
	}

	// There was an issue related to the network, so we're going to mark the
	// region as unavailable, and generate the channel used for announcing
	// when it's available again
	region := rpc.GetRegion()

	if region != nil {
		// reestablishRegion will mark all known regions in this region server
		// as unavailable, and we want to prevent other threads from checking
		// if they should start reestablishRegion before this is done.
		c.regionDownLock.Lock()
		succ := region.MarkUnavailable()
		if succ {
			go c.reestablishRegion(region)
		} else {
			c.regionDownLock.Unlock()
		}
	}

	return c.sendRPC(rpc)
}

// Locates the region in which the given row key for the given table is.
func (c *Client) locateRegion(ctx context.Context, table, key []byte) (*region.Client, *regioninfo.Info, error) {
	metaKey := createRegionSearchKey(table, key)
	rpc := hrpc.NewGetBefore(ctx, metaTableName, metaKey, infoFamily)
	rpc.SetRegion(c.metaRegionInfo)
	resp, err := c.sendRPC(rpc)

	if err != nil {
		ch := c.metaRegionInfo.GetAvailabilityChan()
		if ch != nil {
			select {
			case <-ch:
				return c.locateRegion(ctx, table, key)
			case <-rpc.Context().Done():
				return nil, nil, ErrDeadline
			}
		} else {
			return nil, nil, err
		}
	}

	return c.discoverRegion(ctx, resp.(*pb.GetResponse))
}

type newRegResult struct {
	Client *region.Client
	Err    error
}

var newRegion = func(ret chan newRegResult, host string, port uint16, queueSize int, queueTimeout time.Duration) {
	c, e := region.NewClient(host, port, queueSize, queueTimeout)
	ret <- newRegResult{c, e}
}

// Adds a new region to our regions cache.
func (c *Client) discoverRegion(ctx context.Context, metaRow *pb.GetResponse) (*region.Client, *regioninfo.Info, error) {
	if metaRow.Result == nil {
		return nil, nil, errors.New("table not found")
	}
	var host string
	var port uint16
	var reg *regioninfo.Info
	for _, cell := range metaRow.Result.Cell {
		switch string(cell.Qualifier) {
		case "regioninfo":
			var err error
			reg, err = regioninfo.InfoFromCell(cell)
			if err != nil {
				return nil, nil, err
			}
		case "server":
			value := cell.Value
			if len(value) == 0 {
				continue // Empty during NSRE.
			}
			colon := bytes.IndexByte(value, ':')
			if colon < 1 { // Colon can't be at the beginning.
				return nil, nil,
					fmt.Errorf("broken meta: no colon found in info:server %q", cell)
			}
			host = string(value[:colon])
			portU64, err := strconv.ParseUint(string(value[colon+1:]), 10, 16)
			if err != nil {
				return nil, nil, err
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

	// Check if there's already a client in the cache for talking to this host
	// and port
	client := c.clients.checkForClient(host, port)
	if client != nil {
		c.addRegionToCache(reg, client)
		return client, reg, nil
	}

	var res newRegResult
	ret := make(chan newRegResult)
	go newRegion(ret, host, port, c.rpcQueueSize, c.flushInterval)

	select {
	case res = <-ret:
	case <-ctx.Done():
		return nil, nil, ErrDeadline
	}

	if res.Err != nil {
		return nil, nil, res.Err
	}

	c.addRegionToCache(reg, res.Client)

	return res.Client, reg, nil
}

// Adds a region to our meta cache.
func (c *Client) addRegionToCache(reg *regioninfo.Info, client *region.Client) {
	// Would add more specific information but most fields for reg/client are unexported.
	log.WithFields(log.Fields{
		"Region": reg,
		"Client": client,
	}).Debug("Adding new region to meta cache.")
	// 1. Record the region -> client mapping.
	// This won't be "discoverable" until another map points to it, because
	// at this stage no one knows about this region yet, so another thread
	// may be looking up that region again while we're in the process of
	// publishing our findings.
	c.clients.put(reg, client)

	// 2. Store the region in the sorted map.
	// This will effectively "publish" the result of our work to other
	// threads.  The window between when the previous `put' becomes visible
	// to all other threads and when we're done updating the sorted map is
	// when we may unnecessarily re-lookup the same region again.  It's an
	// acceptable trade-off.  We avoid extra synchronization complexity in
	// exchange of occasional duplicate work (which should be rare anyway).
	c.regions.put(reg.RegionName, reg)
}

// reestablishRegion will continually attempt to reestablish a connection to a
// given region
func (c *Client) reestablishRegion(reg *regioninfo.Info) {
	var regionInfos []*regioninfo.Info

	// First mark all regioninfo.Infos that share the client used by reg as
	// unavailable. If reg is the c.metaRegionInfo, it does not share a client
	// with any other regioninfos.

	if reg != c.metaRegionInfo {
		// This is the client in the cache for the now-unreachable region server
		client := c.clients.get(reg)

		// All regions with the exception of the one passed in need to be marked
		// as unavailable
		regionInfos = c.clients.getInfos(client)
		for _, info := range regionInfos {
			if info != reg {
				marked := info.MarkUnavailable()
				if !marked {
					// If the region was already marked as unavailable, there
					// are multiple reestablishRegions running for the same
					// region
					// TODO: log an error here
				}
			}
		}

		// This region server is inaccessible, and a new client will be created,
		// so the client will be removed from the region client cache for all
		// region infos using it.
		c.clients.delAll(client)

		c.regionDownLock.Unlock()
	}

	// Next, continually retry finding the region server for the given region
	// info and mark any relevant regioninfos as available once found.

	backoffAmount := 16
	for {
		log.WithFields(log.Fields{
			"Table":      reg.Table,
			"RegionName": reg.RegionName,
			"StartKey":   reg.StartKey,
			"StopKey":    reg.StopKey,
		}).Warn("Attempting to re-establish region.")
		// A new context is created here because this is not specific to any
		// request that the user of gohbase initiated, and is instead an
		// internal goroutine that may be servicing any number of requests
		// initiated by the user.
		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
		if reg == c.metaRegionInfo {
			ret := make(chan error)
			go c.locateMeta(ret)

			var err error
			select {
			case err = <-ret:
			case <-ctx.Done():
				continue
			}

			if err == nil {
				c.metaRegionInfo.MarkAvailable()
				return
			}
		} else {
			_, _, err := c.locateRegion(ctx, reg.Table, reg.StartKey)
			if err == nil {
				for _, info := range regionInfos {
					info.MarkAvailable()
				}
				return
			}
		}
		time.Sleep(time.Millisecond * time.Duration(backoffAmount))
		if backoffAmount < 5000 {
			backoffAmount *= 2
		} else {
			backoffAmount += 5000
		}
	}
}

// Looks up the meta region in ZooKeeper.
func (c *Client) locateMeta(ret chan error) {
	host, port, err := zk.LocateMeta(c.zkquorum)
	if err != nil {
		log.Printf("Error while locating meta: %s", err)
		ret <- err
		return
	}
	log.WithFields(log.Fields{
		"Host": host,
		"Port": port,
	}).Debug("Located META from ZooKeeper")
	c.metaClient, err = region.NewClient(host, port, c.rpcQueueSize, c.flushInterval)
	ret <- err
}
