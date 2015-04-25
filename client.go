// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"log"
	"sync"

	"github.com/cznic/b"
	"github.com/tsuna/gohbase/hrpc"
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
		zkquorum: zkquorum,
	}
}

// CheckTable returns an error if the given table name doesn't exist.
func (c *Client) CheckTable(table string) error {
	return c.sendRpcToRegion(hrpc.NewGetStr(table, ""))
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
func (c *Client) sendRpcToRegion(rpc hrpc.Call) error {
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
			return err
		}
	}
	rpc.SetRegion(reg.RegionName)
	return client.SendRpc(rpc)
}

// Locates the region in which the given row key for the given table is.
func (c *Client) locateRegion(table, key []byte) (*region.Client, *region.Info, error) {
	if c.metaClient == nil {
		err := c.locateMeta()
		if err != nil {
			return nil, nil, err
		}
	}
	// TODO
	return c.metaClient, &region.Info{[]byte("aeris"),
		[]byte("aeris,,1430812876256.d810a8bde541afa5ffd03c95923a9854."), []byte{}}, nil
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
