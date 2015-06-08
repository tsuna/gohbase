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
	"sync"

	"github.com/cznic/b"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/zk"
)

// Constants
var (
	// Name of the meta region.
	metaTableName = []byte("hbase:meta")

	metaRegionInfo = &region.Info{
		Table:      []byte("hbase:meta"),
		RegionName: []byte("hbase:meta,,1"),
		StopKey:    []byte{},
	}

	infoFamily = map[string][]string{
		"info": nil,
	}
)

// region -> client cache.
type regionClientCache struct {
	m sync.Mutex

	clients map[*region.Info]*region.Client
}

func (rcc *regionClientCache) get(r *region.Info) *region.Client {
	rcc.m.Lock()
	c := rcc.clients[r]
	rcc.m.Unlock()
	return c
}

func (rcc *regionClientCache) put(r *region.Info, c *region.Client) {
	rcc.m.Lock()
	rcc.clients[r] = c
	rcc.m.Unlock()
}

// key -> region cache.
type keyRegionCache struct {
	m sync.Mutex

	// Maps a []byte of a region start key to a *region.Info
	regions *b.Tree
}

func (krc *keyRegionCache) get(key []byte) ([]byte, *region.Info) {
	krc.m.Lock()
	enum, ok := krc.regions.Seek(key)
	if !ok {
		enum.Prev()
	}
	// TODO: It would be nice if we could do just enum.Get() to avoid the
	// unnecessary cost of seeking to the next entry.
	k, v, err := enum.Prev()
	krc.m.Unlock()
	if err != nil {
		return nil, nil
	}
	return k.([]byte), v.(*region.Info)
}

func (krc *keyRegionCache) put(key []byte, reg *region.Info) {
	krc.m.Lock()
	// TODO: return any value that was there previously etc.
	krc.regions.Set(key, reg)
	krc.m.Unlock()
}

// A Client provides access to an HBase cluster.
type Client struct {
	regions keyRegionCache

	// Maps a *region.Info to the *region.Client that we think currently
	// serves it.
	clients regionClientCache

	// Client connected to the RegionServer hosting the hbase:meta table.
	metaClient *region.Client

	zkquorum string
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string) *Client {
	return &Client{
		regions:  keyRegionCache{regions: b.TreeNew(region.CompareGeneric)},
		clients:  regionClientCache{clients: make(map[*region.Info]*region.Client)},
		zkquorum: zkquorum,
	}
}

// CheckTable returns an error if the given table name doesn't exist.
func (c *Client) CheckTable(table string) (*pb.GetResponse, error) {
	resp, err := c.sendRpcToRegion(hrpc.NewGetStr(table, "theKey", nil))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetResponse), err
}

// GetRow returns a single row fetched from hbase
func (c *Client) GetRow(table string, rowkey string, families map[string][]string) (*pb.GetResponse, error) {
	resp, err := c.sendRpcToRegion(hrpc.NewGetStr(table, rowkey, families))
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetResponse), err
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
func (c *Client) getRegion(table, key []byte) *region.Info {
	if bytes.Equal(table, metaTableName) {
		return metaRegionInfo
	}
	regionName := createRegionSearchKey(table, key)
	key, region := c.regions.get(regionName)
	if region == nil || !isCacheKeyForTable(table, key) {
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
func (c *Client) clientFor(region *region.Info) *region.Client {
	if region == metaRegionInfo {
		return c.metaClient
	}
	return c.clients.get(region)
}

// Sends an RPC targeted at a particular region to the right RegionServer.
// Returns the response (for now, as the call is synchronous).
func (c *Client) sendRpcToRegion(rpc hrpc.Call) (proto.Message, error) {
	table := rpc.Table()
	key := rpc.Key()
	reg := c.getRegion(table, key)

	var client *region.Client
	if reg != nil {
		client = c.clientFor(reg)
	} else {
		var err error
		client, reg, err = c.locateRegion(table, key)
		if err != nil {
			return nil, err
		}
	}
	rpc.SetRegion(reg.RegionName)
	return client.SendRPC(rpc)
}

// Locates the region in which the given row key for the given table is.
func (c *Client) locateRegion(table, key []byte) (*region.Client, *region.Info, error) {
	if c.metaClient == nil {
		err := c.locateMeta()
		if err != nil {
			return nil, nil, err
		}
	}
	metaKey := createRegionSearchKey(table, key)
	rpc := hrpc.NewGetBefore(metaTableName, metaKey, infoFamily)
	rpc.SetRegion(metaRegionInfo.RegionName)
	resp, err := c.metaClient.SendRPC(rpc)
	if err != nil {
		return nil, nil, err
	}
	return c.discoverRegion(resp.(*pb.GetResponse))
}

// Adds a new region to our regions cache.
func (c *Client) discoverRegion(metaRow *pb.GetResponse) (*region.Client, *region.Info, error) {
	if metaRow.Result == nil {
		return nil, nil, errors.New("table not found")
	}
	var host string
	var port uint16
	var reg *region.Info
	for _, cell := range metaRow.Result.Cell {
		switch string(cell.Qualifier) {
		case "regioninfo":
			var err error
			reg, err = region.InfoFromCell(cell)
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

	client, err := region.NewClient(host, port)
	if err != nil {
		return nil, nil, err
	}

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
	return client, reg, nil
}

// Looks up the meta region in ZooKeeper.
func (c *Client) locateMeta() error {
	host, port, err := zk.LocateMeta(c.zkquorum)
	if err != nil {
		log.Printf("Error while locating meta: %s", err)
		return err
	}
	log.Printf("Meta @ %s:%d", host, port)
	c.metaClient, err = region.NewClient(host, port)
	return err
}
