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
	"math"
	"sync"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

const noScannerID = math.MaxUint64

type re struct {
	rs []*pb.Result
	e  error
}

type scanner struct {
	f         fetcher
	once      sync.Once
	cancel    context.CancelFunc
	resultsCh chan re
	// resultsM protext results slice for concurrent calls to Next()
	resultsM sync.Mutex
	results  []*pb.Result
}

func (s *scanner) peek() (*pb.Result, error) {
	if s.f.ctx.Err() != nil {
		return nil, io.EOF
	}
	if len(s.results) == 0 {
		re, ok := <-s.resultsCh
		if !ok {
			return nil, io.EOF
		}
		if re.e != nil {
			return nil, re.e
		}
		// fetcher never returns empty results
		s.results = re.rs
	}
	return s.results[0], nil
}

func (s *scanner) shift() {
	if len(s.results) == 0 {
		return
	}
	// set to nil so that GC isn't blocked to clean up the result
	s.results[0] = nil
	s.results = s.results[1:]
}

// coalesce combines result with partial if they belong to the same row
// and returns the coalesed result and whether coalescing happened
func (s *scanner) coalesce(result, partial *pb.Result) (*pb.Result, bool) {
	if result == nil {
		return partial, true
	}
	if !result.GetPartial() {
		// results is not partial, shouldn't coalesce
		return result, false
	}

	if len(partial.Cell) > 0 && !bytes.Equal(result.Cell[0].Row, partial.Cell[0].Row) {
		// new row
		result.Partial = proto.Bool(false)
		return result, false
	}

	// same row, add the partial
	result.Cell = append(result.Cell, partial.Cell...)
	if partial.GetStale() {
		result.Stale = proto.Bool(partial.GetStale())
	}
	return result, true
}

func newScanner(c RPCClient, rpc *hrpc.Scan) *scanner {
	ctx, cancel := context.WithCancel(rpc.Context())
	resultsCh := make(chan re)
	return &scanner{
		resultsCh: resultsCh,
		cancel:    cancel,
		f: fetcher{
			RPCClient: c,
			rpc:       rpc,
			ctx:       ctx,
			resultsCh: resultsCh,
			startRow:  rpc.StartRow(),
			scannerID: noScannerID,
		},
	}
}

func toLocalResult(r *pb.Result) *hrpc.Result {
	if r == nil {
		return nil
	}
	return hrpc.ToLocalResult(r)
}

// Next returns a row at a time.
// Once all rows are returned, subsequent calls will return nil and io.EOF.
//
// In case of an error or Close() was called, only the first call to Next() will
// return partial result (could be not a complete row) and the actual error,
// the subsequent calls will return nil and io.EOF.
//
// In case a scan rpc has an expired context, partial result and io.EOF will be
// returned. Clients should check the error of the context they passed if they
// want to if the scanner was closed because of the deadline.
//
// This method is thread safe.
func (s *scanner) Next() (*hrpc.Result, error) {
	s.once.Do(func() {
		go s.f.fetch()
	})

	s.resultsM.Lock()

	if s.f.rpc.AllowPartialResults() {
		// if client handles partials, just return it
		r, err := s.peek()
		if err != nil {
			s.resultsM.Unlock()
			return nil, err
		}
		s.shift()
		s.resultsM.Unlock()
		return toLocalResult(r), nil
	}

	var result, partial *pb.Result
	var err error
	for {
		partial, err = s.peek()
		if err == io.EOF && result != nil {
			// no more results, return what we have. Next call to the Next() will get EOF
			result.Partial = proto.Bool(false)
			s.resultsM.Unlock()
			return toLocalResult(result), nil
		}
		if err != nil {
			// return whatever we have so far and the error
			s.resultsM.Unlock()
			return toLocalResult(result), err
		}

		var done bool
		result, done = s.coalesce(result, partial)
		if done {
			s.shift()
		}
		if !result.GetPartial() {
			// if not partial anymore, return it
			s.resultsM.Unlock()
			return toLocalResult(result), nil
		}
	}
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
	resultsCh chan<- re
	// rpc is original scan query
	rpc *hrpc.Scan
	ctx context.Context
	// scannerID is the id of scanner on current region
	scannerID uint64
	// startRow is the start row in the current region
	startRow []byte
}

// trySend sends results and error to results channel. Returns true if scanner is done.
func (f *fetcher) trySend(rs []*pb.Result, err error) bool {
	if err == nil && len(rs) == 0 {
		return false
	}
	select {
	case <-f.ctx.Done():
		return true
	case f.resultsCh <- re{rs: rs, e: err}:
		return false
	}
}

// fetch scans results from appropriate region, sends them to client and updates
// the fetcher for the next scan
func (f *fetcher) fetch() {
	for {
		resp, region, err := f.next()
		if err != nil {
			if err != context.Canceled || err != context.DeadlineExceeded {
				// if the context of the scan rpc wasn't cancelled (same as calling Close()),
				// return the error to client
				f.trySend(nil, err)
			}
			break
		}

		f.update(resp, region)

		if f.trySend(resp.Results, nil) {
			break
		}

		if f.shouldClose(resp, region) {
			break
		}
	}

	close(f.resultsCh)

	if f.scannerID == noScannerID {
		// scanner is automatically closed by hbase
		return
	}

	// if we are closing in the middle of scanning a region,
	// send a close scanner request
	// TODO: add a deadline
	rpc := hrpc.NewCloseFromID(context.Background(),
		f.rpc.Table(), f.scannerID, f.startRow)
	if _, err := f.SendRPC(rpc); err != nil {
		// the best we can do in this case is log. If the request fails,
		// the scanner lease will expired and it will be closed automatically
		// by hbase anyway.
		log.WithFields(log.Fields{
			"err":       err,
			"scannerID": f.scannerID,
		}).Error("failed to close scanner")
	}
}

func (f *fetcher) next() (*pb.ScanResponse, hrpc.RegionInfo, error) {
	var rpc *hrpc.Scan
	var err error
	if f.scannerID == noScannerID {
		// starting to scan on a new region
		rpc, err = hrpc.NewScanRange(
			f.ctx,
			f.rpc.Table(),
			f.startRow,
			f.rpc.StopRow(),
			f.rpc.Options()...,
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

// update updates the fetcher for the next scan request
func (f *fetcher) update(resp *pb.ScanResponse, region hrpc.RegionInfo) {
	if resp.GetMoreResultsInRegion() {
		if resp.ScannerId != nil {
			f.scannerID = resp.GetScannerId()
		}
	} else {
		// we are done with this region, prepare scan for next region
		f.scannerID = noScannerID
		f.startRow = region.StopKey()
	}
}

// shouldClose check if this scanner should be closed and should stop fetching new results
func (f *fetcher) shouldClose(resp *pb.ScanResponse, region hrpc.RegionInfo) bool {
	select {
	case <-f.ctx.Done():
		// scanner has been asked to close
		return true
	default:
	}

	if resp.MoreResults != nil && !*resp.MoreResults {
		// the filter for the whole scan has been exhausted, close the scanner
		return true
	}

	if f.scannerID != noScannerID {
		// not done with this region yet
		return false
	}

	// Check to see if this region is the last we should scan because:
	// (1) it's the last region
	if len(region.StopKey()) == 0 {
		return true
	}
	// (3) because its stop_key is greater than or equal to the stop_key of this scanner,
	// provided that (2) we're not trying to scan until the end of the table.
	return len(f.rpc.StopRow()) != 0 && // (2)
		bytes.Compare(f.rpc.StopRow(), region.StopKey()) <= 0 // (3)
}
