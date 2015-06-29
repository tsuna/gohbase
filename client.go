// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"

	"github.com/cznic/b"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/regioninfo"
	"golang.org/x/net/context"
)

// A Client provides access to an HBase cluster.
type Client struct {
	regions keyRegionCache

	// Maps a *regioninfo.Info to the *region.Client that we think currently
	// serves it.
	clients regionClientCache

	// Client connected to the RegionServer hosting the hbase:meta table.
	metaClient *region.Client

	zkquorum string
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string) *Client {
	return &Client{
		regions:  keyRegionCache{regions: b.TreeNew(regioninfo.CompareGeneric)},
		clients:  regionClientCache{clients: make(map[*regioninfo.Info]*region.Client)},
		zkquorum: zkquorum,
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
