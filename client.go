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

	metaRegionInfo = &regioninfo.Info{
		Table:      []byte("hbase:meta"),
		RegionName: []byte("hbase:meta,,1"),
		StopKey:    []byte{},
	}

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
	rcc.m.Unlock()
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
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string, options ...Option) *Client {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new client.")
	c := &Client{
		regions:       keyRegionCache{regions: b.TreeNew(regioninfo.CompareGeneric)},
		clients:       regionClientCache{clients: make(map[*regioninfo.Info]*region.Client)},
		zkquorum:      zkquorum,
		rpcQueueSize:  100,
		flushInterval: 20,
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
	getStr, _ := hrpc.NewGetStr(ctx, table, "theKey")
	resp, err := c.sendRPC(getStr)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetResponse), err
}

// Get returns a single row fetched from HBase.
func (c *Client) Get(get *hrpc.Get) (*pb.GetResponse, error) {
	resp, err := c.sendRPC(get)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetResponse), err
}

// Scan retrieves the values specified in families from the given range.
func (c *Client) Scan(s *hrpc.Scan) ([]*pb.Result, error) {
	var results []*pb.Result
	var scanres *pb.ScanResponse
	var rpc *hrpc.Scan
	ctx := s.GetContext()
	table := s.Table()
	families := s.GetFamilies()
	startRow := s.GetStartRow()
	stopRow := s.GetStopRow()
	for {
		// Make a new Scan RPC for this region
		if rpc == nil {
			// If it's the first region, just begin at the given startRow
			rpc, _ = hrpc.NewScanRange(ctx, table, startRow, stopRow, hrpc.Families(families))
		} else {
			// If it's not the first region, we want to start at whatever the
			// last region's StopKey was
			rpc, _ = hrpc.NewScanRange(ctx, table, rpc.GetRegionStop(), stopRow, hrpc.Families(families))
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
		return metaRegionInfo
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
	if region == metaRegionInfo {
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

	var client *region.Client
	if reg != nil {
		if reg.IsUnavailable() {
			ch, _ := reg.GetAvailabilityChan()
			select {
			case <-ch:
				return c.queueRPC(rpc)
			case <-rpc.GetContext().Done():
				return ErrDeadline
			}
		}

		client = c.clientFor(reg)
	} else {
		var err error
		client, reg, err = c.locateRegion(rpc.GetContext(), table, key)
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
		"Type":  rpc.GetName(),
		"Table": string(rpc.Table()),
		"Key":   string(rpc.Key()),
	}).Debug("Sending RPC")
	err := c.queueRPC(rpc)
	if err == ErrDeadline {
		return nil, err
	} else if err != nil {
		// There was an error locating the region for the RPC, or the client
		// for the region encountered an error and has shut down.
		return c.sendRPC(rpc)
	}

	var res hrpc.RPCResult
	resch := rpc.GetResultChan()

	select {
	case res = <-resch:
	case <-rpc.GetContext().Done():
		return nil, ErrDeadline
	}

	if res.NetError == nil {
		return res.Msg, res.RPCError
	}

	// There was an issue related to the network, so we're going to mark the
	// region as unavailable, and generate the channel used for announcing
	// when it's available again
	region := rpc.GetRegion()

	if region != nil {
		_, created := region.GetAvailabilityChan()
		if created {
			go c.reestablishRegion(region)
		}
	}

	return c.sendRPC(rpc)
}

// Locates the region in which the given row key for the given table is.
func (c *Client) locateRegion(ctx context.Context, table, key []byte) (*region.Client, *regioninfo.Info, error) {
	if c.metaClient == nil {
		ret := make(chan error)
		go c.locateMeta(ret)

		var err error
		select {
		case err = <-ret:
		case <-ctx.Done():
			return nil, nil, ErrDeadline
		}

		if err != nil {
			return nil, nil, err
		}
	}
	metaKey := createRegionSearchKey(table, key)
	rpc, _ := hrpc.NewGetBefore(ctx, metaTableName, metaKey, hrpc.Families(infoFamily))
	rpc.SetRegion(metaRegionInfo)
	resp, err := c.sendRPC(rpc)
	if err != nil {
		return nil, nil, err
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
		_, _, err := c.locateRegion(context.Background(), reg.Table, reg.StartKey)
		if err == nil {
			reg.MarkAvailable()
			return
		}
		// TODO: Make this configurable, or verify that it's a sane number
		time.Sleep(time.Millisecond * 100)
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
