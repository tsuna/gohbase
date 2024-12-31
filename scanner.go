// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"time"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

const (
	noScannerID = math.MaxUint64

	rowsScanned  = "ROWS_SCANNED"
	rowsFiltered = "ROWS_FILTERED"
)

// rowPadding used to pad the row key when constructing a row before
var rowPadding = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

type scanner struct {
	RPCClient
	// rpc is original scan query
	rpc *hrpc.Scan
	// curRegionScannerID is the id of scanner on current region
	curRegionScannerID uint64
	// startRow is the start row in the current region
	startRow    []byte
	results     []*pb.Result
	closed      bool
	scanMetrics map[string]int64

	logger      *slog.Logger
	renewCancel context.CancelFunc
}

func (s *scanner) fetch() ([]*pb.Result, error) {
	// keep looping until we have error, some non-empty result or until close
	for {
		resp, region, err := s.request()
		if err != nil {
			s.Close()
			return nil, err
		}
		if s.rpc.TrackScanMetrics() && resp.ScanMetrics != nil {
			metrics := resp.ScanMetrics.GetMetrics()
			for _, m := range metrics {
				s.scanMetrics[m.GetName()] += m.GetValue()
			}
		}

		s.update(resp, region)
		if s.isDone(resp, region) {
			s.Close()
		}

		if rs := resp.Results; len(rs) > 0 {
			return rs, nil
		} else if s.closed {
			return nil, io.EOF
		}
	}
}

func (s *scanner) peek() (*pb.Result, error) {
	if len(s.results) > 0 {
		return s.results[0], nil
	}

	if s.renewCancel != nil {
		// About to send new Scan request to HBase, cancel our
		// renewer.
		s.renewCancel()
		s.renewCancel = nil
	}

	if s.closed {
		// done scanning
		return nil, io.EOF
	}

	rs, err := s.fetch()
	if err != nil {
		return nil, err
	}
	if !s.closed && s.rpc.RenewInterval() > 0 {
		// Start up a renewer
		renewCtx, cancel := context.WithCancel(s.rpc.Context())
		s.renewCancel = cancel
		go s.renewLoop(renewCtx, s.startRow)
	}

	// fetch cannot return zero results
	s.results = rs
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
// and returns the coalesced result and whether coalescing happened
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

func newScanner(c RPCClient, rpc *hrpc.Scan, logger *slog.Logger) *scanner {
	var sm map[string]int64
	if rpc.TrackScanMetrics() {
		sm = make(map[string]int64)
	}
	return &scanner{
		RPCClient:          c,
		rpc:                rpc,
		startRow:           rpc.StartRow(),
		curRegionScannerID: noScannerID,
		scanMetrics:        sm,
		logger:             logger,
	}
}

func toLocalResult(r *pb.Result) *hrpc.Result {
	if r == nil {
		return nil
	}
	res := hrpc.ToLocalResult(r)
	return res
}

func (s *scanner) Next() (*hrpc.Result, error) {
	var (
		result, partial *pb.Result
		err             error
	)

	select {
	case <-s.rpc.Context().Done():
		s.Close()
		return nil, s.rpc.Context().Err()
	default:
	}

	if s.rpc.AllowPartialResults() {
		// if client handles partials, just return it
		result, err = s.peek()
		if err != nil {
			return nil, err
		}
		s.shift()
		return toLocalResult(result), nil
	}

	for {
		partial, err = s.peek()
		if err == io.EOF && result != nil {
			// no more results, return what we have. Next call to the Next() will get EOF
			result.Partial = proto.Bool(false)
			return toLocalResult(result), nil
		}
		if err != nil {
			// return whatever we have so far and the error
			return toLocalResult(result), err
		}

		var done bool
		result, done = s.coalesce(result, partial)
		if done {
			s.shift()
		}
		if !result.GetPartial() {
			// if not partial anymore, return it
			return toLocalResult(result), nil
		}
	}
}

func (s *scanner) request() (*pb.ScanResponse, hrpc.RegionInfo, error) {
	var (
		rpc *hrpc.Scan
		err error
	)

	if s.isRegionScannerClosed() {
		// open a new region scan to scan on a new region
		rpc, err = hrpc.NewScanRange(
			s.rpc.Context(),
			s.rpc.Table(),
			s.startRow,
			s.rpc.StopRow(),
			s.rpc.Options()...)
	} else {
		// continuing to scan current region
		rpc, err = hrpc.NewScanRange(s.rpc.Context(),
			s.rpc.Table(),
			s.startRow,
			nil,
			hrpc.ScannerID(s.curRegionScannerID),
			hrpc.NumberOfRows(s.rpc.NumberOfRows()),
			hrpc.Priority(s.rpc.Priority()),
			hrpc.RenewInterval(s.rpc.RenewInterval()),
		)
	}
	if err != nil {
		return nil, nil, err
	}

	res, err := s.SendRPC(rpc)
	if err != nil {
		return nil, nil, err
	}
	scanres, ok := res.(*pb.ScanResponse)
	if !ok {
		return nil, nil, errors.New("got non-ScanResponse for scan request")
	}
	return scanres, rpc.Region(), nil
}

// update updates the scanner for the next scan request
func (s *scanner) update(resp *pb.ScanResponse, region hrpc.RegionInfo) {
	if s.isRegionScannerClosed() && resp.ScannerId != nil {
		s.openRegionScanner(resp.GetScannerId())
	}
	if !resp.GetMoreResultsInRegion() {
		// we are done with this region, prepare scan for next region
		s.curRegionScannerID = noScannerID

		// Normal Scan
		if !s.rpc.Reversed() {
			s.startRow = region.StopKey()
			return
		}

		// Reversed Scan
		// return if we are at the end
		if len(region.StartKey()) == 0 {
			s.startRow = region.StartKey()
			return
		}

		// create the nearest value lower than the current region startKey
		rsk := region.StartKey()
		// if last element is 0x0, just shorten the slice
		if rsk[len(rsk)-1] == 0x0 {
			s.startRow = rsk[:len(rsk)-1]
			return
		}

		// otherwise lower the last element byte value by 1 and pad with 0xffs
		tmp := make([]byte, len(rsk), len(rsk)+len(rowPadding))
		copy(tmp, rsk)
		tmp[len(tmp)-1] = tmp[len(tmp)-1] - 1
		s.startRow = append(tmp, rowPadding...)
	}
}

func (s *scanner) Close() error {
	if s.closed {
		return nil
	}
	if s.renewCancel != nil {
		s.renewCancel()
	}
	s.closed = true
	// close the last region scanner
	s.closeRegionScanner()
	return nil
}

// GetScanMetrics returns the scan metrics for the scanner.
// The scan metrics are non-nil only if the Scan has TrackScanMetrics() enabled.
// GetScanMetrics should only be called after the scanner has been closed with an io.EOF
// (there are no more rows left to be returned by calls to Next()).
func (s *scanner) GetScanMetrics() map[string]int64 {
	return s.scanMetrics
}

// isDone check if this scanner is done fetching new results
func (s *scanner) isDone(resp *pb.ScanResponse, region hrpc.RegionInfo) bool {
	if resp.MoreResults != nil && !*resp.MoreResults {
		// or the filter for the whole scan has been exhausted, close the scanner
		return true
	}

	if !s.isRegionScannerClosed() {
		// not done with this region yet
		return false
	}

	// Check to see if this region is the last we should scan because:
	// (1) it's the last region
	if len(region.StopKey()) == 0 && !s.rpc.Reversed() {
		return true
	}
	if s.rpc.Reversed() && len(region.StartKey()) == 0 {
		return true
	}
	// (3) because its stop_key is greater than or equal to the stop_key of this scanner,
	// provided that (2) we're not trying to scan until the end of the table.
	if !s.rpc.Reversed() {
		return len(s.rpc.StopRow()) != 0 && // (2)
			bytes.Compare(s.rpc.StopRow(), region.StopKey()) <= 0 // (3)
	}

	//  Reversed Scanner
	return len(s.rpc.StopRow()) != 0 && // (2)
		bytes.Compare(s.rpc.StopRow(), region.StartKey()) >= 0 // (3)
}

func (s *scanner) isRegionScannerClosed() bool {
	return s.curRegionScannerID == noScannerID
}

func (s *scanner) openRegionScanner(scannerId uint64) {
	if !s.isRegionScannerClosed() {
		panic(fmt.Sprintf("should not happen: previous region scanner was not closed"))
	}
	s.curRegionScannerID = scannerId
}

func (s *scanner) closeRegionScanner() {
	if s.isRegionScannerClosed() {
		return
	}
	if !s.rpc.IsClosing() {
		// Not closed at server side
		// if we are closing in the middle of scanning a region,
		// send a close scanner request
		// TODO: add a deadline
		rpc, err := hrpc.NewScanRange(context.Background(),
			s.rpc.Table(), s.startRow, nil,
			hrpc.ScannerID(s.curRegionScannerID),
			hrpc.CloseScanner(),
			hrpc.NumberOfRows(0))
		if err != nil {
			panic(fmt.Sprintf("should not happen: %s", err))
		}

		// If the request fails, the scanner lease will be expired
		// and it will be closed automatically by hbase.
		// No need to bother clients about that.
		go s.SendRPC(rpc)
	}
	s.curRegionScannerID = noScannerID
}

// renews a scanner by resending scan request with renew = true
func (s *scanner) renew(ctx context.Context, startRow []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rpc, err := hrpc.NewScanRange(ctx,
		s.rpc.Table(),
		startRow,
		nil,
		hrpc.ScannerID(s.curRegionScannerID),
		hrpc.Priority(s.rpc.Priority()),
		hrpc.RenewalScan(),
	)
	if err != nil {
		return err
	}
	_, err = s.SendRPC(rpc)
	return err
}

func (s *scanner) renewLoop(ctx context.Context, startRow []byte) {
	scanRenewers.Inc()
	t := time.NewTicker(s.rpc.RenewInterval())
	defer func() {
		t.Stop()
		scanRenewers.Dec()
	}()

	for {
		select {
		case <-t.C:
			if err := s.renew(ctx, startRow); err != nil {
				s.logger.Error("error renewing scanner", "err", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
