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

	log "github.com/Sirupsen/logrus"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

const noScannerID = math.MaxUint64

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
			scannerID: noScannerID,
		},
	}
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
	// scannerID is the id of scanner on current region
	scannerID uint64
	// startRow is the start row in the current region
	startRow []byte
	// result current result we are adding partials to
	result *hrpc.Result
}

// coalese returns a complete row or nil in case more partial results
// are expected
func (f *fetcher) coalese(partial *hrpc.Result) *hrpc.Result {
	if partial == nil {
		return nil
	}

	if f.result == nil {
		if len(partial.Cells) == 0 {
			return nil
		}
		if !partial.Partial {
			return partial
		}
		f.result = partial
		return nil
	}

	if len(partial.Cells) > 0 && !bytes.Equal(f.result.Cells[0].Row, partial.Cells[0].Row) {
		// new row
		result := f.result
		result.Partial = false
		f.result = partial
		return result
	}

	// same row, add the partial
	f.result.Cells = append(f.result.Cells, partial.Cells...)
	if partial.Stale {
		f.result.Stale = partial.Stale
	}
	if f.scannerID == noScannerID {
		// no more results in this region, return whatever we have,
		// rows cannot go across region boundaries
		result := f.result
		result.Partial = false
		f.result = nil
		return result
	}
	return nil
}

// send sends a result and error to results channel. Returns true if scanner is done.
func (f *fetcher) send(r *hrpc.Result, err error) bool {
	if f.rpc.AllowPartialResults() {
		return f.trySend(r, err)
	}

	r = f.coalese(r)
	if r == nil {
		if err == nil {
			// nothing to be sent yet
			return false
		} else {
			// if there's an error, return whatever result we have
			r = f.result
		}
	}
	return f.trySend(r, err)
}

func (f *fetcher) trySend(r *hrpc.Result, err error) bool {
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
	for {
		resp, region, err := f.next()
		if err != nil {
			if err != ErrDeadline {
				// if the context of the scan rpc wasn't cancelled (same as calling Close()),
				// return the error to client
				f.send(nil, err)
			}
			break
		}

		f.update(resp, region)

		for _, result := range resp.Results {
			if f.send(hrpc.ToLocalResult(result), nil) {
				break
			}
		}

		// check whether we should close the scanner before making next request
		if f.shouldClose(resp, region) {
			if f.result != nil {
				// if there were results, send the last row
				f.trySend(f.result, nil)
			}
			break
		}
	}

	close(f.results)

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
			hrpc.MaxResultSize(f.rpc.MaxResultSize()),
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
