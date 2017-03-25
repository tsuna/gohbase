// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

type re struct {
	r *hrpc.Result
	e error
}

type scanner struct {
	// fetcher's fileds shouldn't be accessed by scanner
	// TODO: maybe separate fetcher into a different package
	f       fetcher
	once    sync.Once
	results chan re
	cancel  context.CancelFunc
}

func newScanner(c RPCClient, rpc *hrpc.Scan) *scanner {
	ctx, cancel := context.WithCancel(rpc.Context())
	results := make(chan re)
	return &scanner{
		results: results,
		cancel:  cancel,
		f: fetcher{
			RPCClient: c,
			rpc:       rpc,
			ctx:       ctx,
			results:   results,
			startRow:  rpc.StartRow(),
		},
	}
}

// Next returns a row at a time.
// This method is thread safe. In case of an error, only the first call to Next()
// will return the actual error, the subsequent calls will return io.EOF.
// In case a scan rpc has an expired context, io.EOF will be returned as well.
// Clients should check the error of the context they passed if they want to know
// why the scanner was actually closed.
func (s *scanner) Next() (*hrpc.Result, error) {
	s.once.Do(func() {
		go s.f.fetch()
	})

	re, ok := <-s.results
	if !ok {
		return nil, io.EOF
	}
	return re.r, re.e
}

// Close should be called if it is desired to stop scanning before getting all of results.
// If you call Next() after calling Close() you might still get buffered results.
// Othwerwise, in case all results have been delivered or in case of an error, the Scanner
// will be closed automatically.
func (s *scanner) Close() error {
	s.cancel()
	return nil
}

type fetcher struct {
	RPCClient
	results chan<- re
	// rpc is original scan query
	rpc *hrpc.Scan
	ctx context.Context
	// region is the current region being scanned
	region hrpc.RegionInfo
	// moreResultsInRegion tells whether there are more results in current region
	moreResultsInRegion bool
	// scannerID is the id of scanner on current region
	scannerID uint64
	// startRow is the start row in the current region
	startRow []byte
}

// send sends a result and error to results channel. Returns true if scanner is done.
func (f *fetcher) send(r *hrpc.Result, err error) bool {
	select {
	case <-f.ctx.Done():
		return true
	case f.results <- re{r: r, e: err}:
		return false
	}
}

// fetch scans results from appropriate region, sends them to client and updates
// the fetcher for the next scan
func (f *fetcher) fetch() {
	defer func() {
		if !f.moreResultsInRegion {
			// scanner is automatically closed by hbase
			return
		}
		// if we are closing in the middle of scanning a region,
		// send a close scanner request
		// TODO: add a deadline
		rpc := hrpc.NewCloseFromID(context.Background(),
			f.region.Table(), f.scannerID, f.region.StartKey())
		if _, err := f.SendRPC(rpc); err != nil {
			// the best we can do in this case is log. If the request fails,
			// the scanner lease will expired and it will be closed automatically
			// by hbase anyway.
			log.WithFields(log.Fields{
				"err":       err,
				"scannerID": f.scannerID,
			}).Error("failed to close scanner")
		}
	}()
	defer close(f.results)
	for {
		resp, region, err := f.next()
		if err != nil {
			if err != ErrDeadline {
				// if the context of the scan rpc wasn't cancelled (same as calling Close()),
				// return the error to client
				f.send(nil, err)
			}
			return
		}

		f.update(resp, region)

		for _, result := range resp.Results {
			if f.send(hrpc.ToLocalResult(result), nil) {
				return
			}
		}

		// check whether we should close the scanner before making next request
		if f.shouldClose() {
			return
		}
	}
}

func (f *fetcher) next() (*pb.ScanResponse, hrpc.RegionInfo, error) {
	var rpc *hrpc.Scan
	var err error
	if !f.moreResultsInRegion {
		// starting to scan on a new region
		from, to := f.rpc.TimeRange()
		rpc, err = hrpc.NewScanRange(
			f.ctx,
			f.rpc.Table(),
			f.startRow,
			f.rpc.StopRow(),
			hrpc.Families(f.rpc.Families()),
			hrpc.Filters(f.rpc.Filter()),
			hrpc.TimeRangeUint64(from, to),
			hrpc.MaxVersions(f.rpc.MaxVersions()),
			hrpc.NumberOfRows(f.rpc.NumberOfRows()),
		)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// continuing to scan current region
		rpc = hrpc.NewScanFromID(f.ctx, f.rpc.Table(), f.scannerID, f.startRow)
	}

	res, err := f.SendRPC(rpc)
	if err != nil {
		return nil, nil, err
	}
	scanres, ok := res.(*pb.ScanResponse)
	if !ok {
		return nil, nil, errors.New("got non-ScanResponse for scan request")
	}
	return scanres, rpc.Region(), nil
}

// update updates the fetcher for the next scan request. Returns true if scanner is done.
func (f *fetcher) update(resp *pb.ScanResponse, region hrpc.RegionInfo) {
	if resp.GetMoreResultsInRegion() {
		f.scannerID = *resp.ScannerId
	} else {
		// prepare for next scan request
		f.scannerID = 0
		f.startRow = region.StopKey()
	}
	f.region = region
	f.moreResultsInRegion = resp.GetMoreResultsInRegion()
}

// shouldClose check if this scanner should be closed and should stop fetching new results
func (f *fetcher) shouldClose() bool {
	select {
	case <-f.ctx.Done():
		// scanner has been asked to close
		return true
	default:
	}

	if f.moreResultsInRegion {
		return false
	}

	// Check to see if this region is the last we should scan because:
	// (1) it's the last region
	if len(f.region.StopKey()) == 0 {
		return true
	}
	// (3) because its stop_key is greater than or equal to the stop_key of this scanner,
	// provided that (2) we're not trying to scan until the end of the table.
	return len(f.rpc.StopRow()) != 0 && // (2)
		bytes.Compare(f.rpc.StopRow(), f.region.StopKey()) <= 0 // (3)
}
