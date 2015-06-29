// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

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
	case <-rpc.Context().Done():
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
	var err error
	if c.metaClient == nil {
		ret := make(chan error)
		go c.locateMeta(ret)

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
	rpc := hrpc.NewGetBefore(ctx, metaTableName, metaKey, infoFamily)
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

var newRegion = func(ret chan newRegResult, host string, port uint16) {
	c, e := region.NewClient(host, port)
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
	go newRegion(ret, host, port)

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
		// A new context is created here because this is not specific to any
		// request that the user of gohbase initiated, and is instead an
		// internal goroutine that may be servicing any number of requests
		// initiated by the user.
		_, _, err := c.locateRegion(context.Background(), reg.Table, reg.StartKey)
		if err == nil {
			ch, _ := reg.GetAvailabilityChan()
			close(ch)
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
	log.Printf("Meta @ %s:%d", host, port)
	c.metaClient, err = region.NewClient(host, port)
	ret <- err
}
