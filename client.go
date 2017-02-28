// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cznic/b"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/internal/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/zk"
)

const (
	standardClient = iota
	adminClient
	defaultRPCQueueSize  = 100
	defaultFlushInterval = 20 * time.Millisecond
	defaultZkRoot        = "/hbase"
	defaultEffectiveUser = "root"
)

// Client a regular HBase client
type Client interface {
	Scan(s *hrpc.Scan) ([]*hrpc.Result, error)
	Get(g *hrpc.Get) (*hrpc.Result, error)
	Put(p *hrpc.Mutate) (*hrpc.Result, error)
	Delete(d *hrpc.Mutate) (*hrpc.Result, error)
	Append(a *hrpc.Mutate) (*hrpc.Result, error)
	Increment(i *hrpc.Mutate) (int64, error)
	CheckAndPut(p *hrpc.Mutate, family string, qualifier string,
		expectedValue []byte) (bool, error)
	Close()
}

type Option func(*client)

// A Client provides access to an HBase cluster.
type client struct {
	clientType int

	regions keyRegionCache

	// Maps a hrpc.RegionInfo to the *region.Client that we think currently
	// serves it.
	clients clientRegionCache

	metaRegionInfo hrpc.RegionInfo

	adminRegionInfo hrpc.RegionInfo

	// The maximum size of the RPC queue in the region client
	rpcQueueSize int

	// zkClient is zookeeper for retrieving meta and admin information
	zkClient zk.Client

	// The root zookeeper path for Hbase. By default, this is usually "/hbase".
	zkRoot string

	// The timeout before flushing the RPC queue in the region client
	flushInterval time.Duration

	// The user used when accessing regions.
	effectiveUser string
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string, options ...Option) Client {
	return newClient(zkquorum, options...)
}

func newClient(zkquorum string, options ...Option) *client {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new client.")
	c := &client{
		clientType: standardClient,
		regions:    keyRegionCache{regions: b.TreeNew(region.CompareGeneric)},
		clients: clientRegionCache{
			regions: make(map[hrpc.RegionClient]map[hrpc.RegionInfo]struct{}),
		},
		rpcQueueSize:  defaultRPCQueueSize,
		flushInterval: defaultFlushInterval,
		metaRegionInfo: region.NewInfo(
			0,
			[]byte("hbase"),
			[]byte("meta"),
			[]byte("hbase:meta,,1"),
			nil,
			nil),
		zkRoot:        defaultZkRoot,
		zkClient:      zk.NewClient(zkquorum),
		effectiveUser: defaultEffectiveUser,
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// RpcQueueSize will return an option that will set the size of the RPC queues
// used in a given client
func RpcQueueSize(size int) Option {
	return func(c *client) {
		c.rpcQueueSize = size
	}
}

// ZookeeperRoot will return an option that will set the zookeeper root path used in a given client.
func ZookeeperRoot(root string) Option {
	return func(c *client) {
		c.zkRoot = root
	}
}

// EffectiveUser will return an option that will set the user used when accessing regions.
func EffectiveUser(user string) Option {
	return func(c *client) {
		c.effectiveUser = user
	}
}

// FlushInterval will return an option that will set the timeout for flushing
// the RPC queues used in a given client
func FlushInterval(interval time.Duration) Option {
	return func(c *client) {
		c.flushInterval = interval
	}
}

// Close closes connections to hbase master and regionservers
func (c *client) Close() {
	// TODO: do we need a lock for metaRegionInfo and adminRegionInfo
	if c.clientType == adminClient {
		if ac := c.adminRegionInfo.Client(); ac != nil {
			ac.Close()
		}
	} else {
		if mc := c.metaRegionInfo.Client(); mc != nil {
			mc.Close()
		}
	}
	c.clients.closeAll()
}

// Scan retrieves the values specified in families from the given range.
func (c *client) Scan(s *hrpc.Scan) ([]*hrpc.Result, error) {
	var results []*pb.Result
	var scanres *pb.ScanResponse
	var rpc *hrpc.Scan
	ctx := s.Context()
	table := s.Table()
	families := s.Families()
	filters := s.Filter()
	startRow := s.StartRow()
	stopRow := s.StopRow()
	fromTs, toTs := s.TimeRange()
	maxVersions := s.MaxVersions()
	numberOfRows := s.NumberOfRows()
	for {
		if rpc != nil {
			// If it's not the first region, we want to start at whatever the
			// last region's StopKey was
			startRow = rpc.RegionStop()
		}
		regionRowLimit := numberOfRows - uint32(len(results))

		// TODO: would be nicer to clone it in some way
		var err error
		rpc, err = hrpc.NewScanRange(ctx, table, startRow, stopRow,
			hrpc.Families(families), hrpc.Filters(filters),
			hrpc.TimeRangeUint64(fromTs, toTs),
			hrpc.MaxVersions(maxVersions),
			hrpc.NumberOfRows(regionRowLimit))
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
		for len(scanres.Results) != 0 && uint32(len(results)) < numberOfRows {
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

		// Check to see if this region is the last we should scan because:
		// (0) we got the number of rows user asked for
		// (1) it's the last region or
		// (3) because its stop_key is greater than or equal to the stop_key of this scanner,
		// provided that (2) we're not trying to scan until the end of the table.
		if uint32(len(results)) >= numberOfRows ||
			// (1)
			len(rpc.RegionStop()) == 0 ||
			// (2)                (3)
			(len(stopRow) != 0 && bytes.Compare(stopRow, rpc.RegionStop()) <= 0) {
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

func (c *client) Get(g *hrpc.Get) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(g)
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.GetResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a GetResponse")
	}

	return hrpc.ToLocalResult(r.Result), nil
}

func (c *client) Put(p *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(p)
}

func (c *client) Delete(d *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(d)
}

func (c *client) Append(a *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(a)
}

func (c *client) Increment(i *hrpc.Mutate) (int64, error) {
	r, err := c.mutate(i)
	if err != nil {
		return 0, err
	}

	if len(r.Cells) != 1 {
		return 0, fmt.Errorf("increment returned %d cells, but we expected exactly one",
			len(r.Cells))
	}

	val := binary.BigEndian.Uint64(r.Cells[0].Value)
	return int64(val), nil
}

func (c *client) mutate(m *hrpc.Mutate) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(m)
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.MutateResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a MutateResponse")
	}

	return hrpc.ToLocalResult(r.Result), nil
}

func (c *client) CheckAndPut(p *hrpc.Mutate, family string,
	qualifier string, expectedValue []byte) (bool, error) {
	cas, err := hrpc.NewCheckAndPut(p, family, qualifier, expectedValue)
	if err != nil {
		return false, err
	}

	pbmsg, err := c.sendRPC(cas)
	if err != nil {
		return false, err
	}

	r, ok := pbmsg.(*pb.MutateResponse)
	if !ok {
		return false, fmt.Errorf("sendRPC returned a %T instead of MutateResponse", pbmsg)
	}

	if r.Processed == nil {
		return false, fmt.Errorf("protobuf in the response didn't contain the field "+
			"indicating whether the CheckAndPut was successful or not: %s", r)
	}

	return r.GetProcessed(), nil
}
