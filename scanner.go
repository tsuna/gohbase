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
	startRow []byte

	response    *hrpc.ScanResponseV2
	resultIndex int

	closed      bool
	scanMetrics map[string]int64

	logger      *slog.Logger
	renewCancel context.CancelFunc
}

type scannerV2 struct {
	scanner
}

func (s *scanner) fetch() (*hrpc.ScanResponseV2, error) {
	// keep looping until we have error, some non-empty result or until close
	for {
		rpc, err := s.makeScanRequest(0)
		if err != nil {
			s.Close()
			return nil, err
		}

		response, err := s.request(rpc)
		if err != nil {
			return nil, err
		}

		if response != nil && len(response.Results) > 0 {
			return response, nil
		} else if s.closed {
			return nil, io.EOF
		}
	}
}

func (s *scanner) peek() (hrpc.ResultV2, error) {
	if s.response != nil {
		return s.response.Results[s.resultIndex], nil
	}

	if s.renewCancel != nil {
		// About to send new Scan request to HBase, cancel our
		// renewer.
		s.renewCancel()
		s.renewCancel = nil
	}

	if s.closed {
		// done scanning
		return hrpc.ResultV2{}, io.EOF
	}

	rs, err := s.fetch()
	if err != nil {
		return hrpc.ResultV2{}, err
	}
	if !s.isRegionScannerClosed() && s.rpc.RenewInterval() > 0 {
		// Start up a renewer if there is more to read within this
		// region.
		renewCtx, cancel := context.WithCancel(s.rpc.Context())
		s.renewCancel = cancel
		go s.renewLoop(renewCtx, s.curRegionScannerID, s.startRow)
	}

	// fetch cannot return zero results
	s.response = rs
	return s.response.Results[0], nil
}

func (s *scanner) shift() {
	if s.response == nil {
		return
	}
	s.resultIndex++
	if s.resultIndex == len(s.response.Results) {
		s.response = nil
		s.resultIndex = 0
	}
}

// coalesce combines result with partial if they belong to the same row
// and returns the coalesced result and whether coalescing happened
func (s *scanner) coalesce(result, partial hrpc.ResultV2) (hrpc.ResultV2, bool) {
	if len(result.Cells) == 0 {
		return partial, true
	}
	if !result.Partial {
		// results is not partial, shouldn't coalesce
		return result, false
	}

	if len(partial.Cells) > 0 && !bytes.Equal(result.Cells[0].Row(), partial.Cells[0].Row()) {
		// new row
		result.Partial = false
		return result, false
	}

	// same row, add the partial
	result.Cells = append(result.Cells, partial.Cells...)
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

func newScannerV2(c RPCClient, rpc *hrpc.Scan, logger *slog.Logger) *scannerV2 {
	var sm map[string]int64
	if rpc.TrackScanMetrics() {
		sm = make(map[string]int64)
	}
	return &scannerV2{
		scanner: scanner{
			RPCClient:          c,
			rpc:                rpc,
			startRow:           rpc.StartRow(),
			curRegionScannerID: noScannerID,
			scanMetrics:        sm,
			logger:             logger,
		},
	}
}

func (s *scanner) Next() (*hrpc.Result, error) {
	if err := s.rpc.Context().Err(); err != nil {
		return nil, err
	}

	if s.rpc.AllowPartialResults() {
		// if client handles partials, just return it
		result, err := s.peek()
		if err != nil {
			return nil, err
		}
		s.shift()
		return hrpc.ResultV2ToResult(result), nil
	}

	var (
		result, partial hrpc.ResultV2
		err             error
	)

	for {
		partial, err = s.peek()
		if err == io.EOF && len(result.Cells) > 0 {
			// no more results, return what we have. Next call to the Next() will get EOF
			result.Partial = false
			return hrpc.ResultV2ToResult(result), nil
		}
		if err != nil {
			// return whatever we have so far and the error
			return hrpc.ResultV2ToResult(result), err
		}

		var done bool
		result, done = s.coalesce(result, partial)
		if done {
			s.shift()
		}
		if !result.Partial {
			// if not partial anymore, return it
			return hrpc.ResultV2ToResult(result), nil
		}
	}
}

func (s *scannerV2) Next() (*hrpc.ScanResponseV2, error) {
	if s.renewCancel != nil {
		// About to send new Scan request to HBase, cancel our
		// renewer.
		s.renewCancel()
		s.renewCancel = nil
	}

	if s.closed {
		return nil, io.EOF
	}

	if err := s.rpc.Context().Err(); err != nil {
		return nil, err
	}
	resp, err := s.fetch()
	if err != nil {
		return nil, err
	}
	if !s.isRegionScannerClosed() && s.rpc.RenewInterval() > 0 {
		// Start up a renewer if there is more to read within this
		// region.
		renewCtx, cancel := context.WithCancel(s.rpc.Context())
		s.renewCancel = cancel
		go s.renewLoop(renewCtx, s.curRegionScannerID, s.startRow)
	}
	return resp, nil
}

// Scan is a lower-level version of Next. Unlike Next, Scan may
// return a Response with 0 results and a nil error. One reason
// this can occur is if HBase exceeds the configured scanner
// timeout without finding any results. Scan allows modifying the
// number of rows to be read. Use a value of 0 rows to defer to
// the configuration in the hrpc.Scan.
func (s *scannerV2) Scan(rows uint32) (*hrpc.ScanResponseV2, error) {
	if s.renewCancel != nil {
		// About to send new Scan request to HBase, cancel our
		// renewer.
		s.renewCancel()
		s.renewCancel = nil
	}

	if s.closed {
		return nil, io.EOF
	}

	if err := s.rpc.Context().Err(); err != nil {
		return nil, err
	}

	rpc, err := s.makeScanRequest(rows)
	if err != nil {
		s.Close()
		return nil, err
	}

	response, err := s.request(rpc)
	if err != nil {
		return nil, err
	}

	if !s.isRegionScannerClosed() && s.rpc.RenewInterval() > 0 {
		// Start up a renewer if there is more to read within this
		// region.
		renewCtx, cancel := context.WithCancel(s.rpc.Context())
		s.renewCancel = cancel
		go s.renewLoop(renewCtx, s.curRegionScannerID, s.startRow)
	}

	return response, nil
}

// makeScanRequest creates the next scan request. The rows argument
// configures the maximum number of rows HBase will return in the
// response. If rows is zero then we defer to the original scan
// requests rows configuration.
func (s *scanner) makeScanRequest(rows uint32) (*hrpc.Scan, error) {
	if s.isRegionScannerClosed() {
		// preserve ScanStatsID
		opts := append(s.rpc.Options(), hrpc.ScanStatsID(s.rpc.ScanStatsID()))
		if rows > 0 {
			opts = append(opts, hrpc.NumberOfRows(rows))
		}

		// open a new region scan to scan on a new region
		return hrpc.NewScanRange(
			s.rpc.Context(),
			s.rpc.Table(),
			s.startRow,
			s.rpc.StopRow(),
			opts...)
	}

	numberOfRows := rows
	if numberOfRows == 0 {
		numberOfRows = s.rpc.NumberOfRows()
	}

	// continuing to scan current region
	opts := []func(hrpc.Call) error{
		hrpc.ScannerID(s.curRegionScannerID),
		hrpc.NumberOfRows(numberOfRows),
		hrpc.Priority(s.rpc.Priority()),
		hrpc.RenewInterval(s.rpc.RenewInterval()),
		// preserve ScanStatsID
		hrpc.ScanStatsID(s.rpc.ScanStatsID()),
		hrpc.WithScanStatsHandler(s.rpc.ScanStatsHandler()),
	}
	if s.rpc.TrackScanMetrics() {
		opts = append(opts, hrpc.TrackScanMetrics())
	}

	return hrpc.NewScanRange(s.rpc.Context(),
		s.rpc.Table(),
		s.startRow,
		nil,
		opts...,
	)
}

// request sends the next scan request to HBase and calls update() to
// prepare for the next request.
func (s *scanner) request(rpc *hrpc.Scan) (*hrpc.ScanResponseV2, error) {
	res, err := s.SendRPC(rpc)
	if err != nil {
		s.Close()
		return nil, err
	}
	scanres, ok := res.(*pb.ScanResponse)
	if !ok {
		s.Close()
		return nil, errors.New("got non-ScanResponse for scan request")
	}

	if s.rpc.TrackScanMetrics() && scanres.ScanMetrics != nil {
		metrics := scanres.ScanMetrics.GetMetrics()
		for _, m := range metrics {
			s.scanMetrics[m.GetName()] += m.GetValue()
		}
	}

	s.update(scanres, rpc.Region())

	return rpc.Response, nil
}

// update updates the scanner for the next scan request or closes it.
func (s *scanner) update(resp *pb.ScanResponse, region hrpc.RegionInfo) {
	// Either there are more results in this region, more results in
	// other regions, or no more results.
	if resp.GetMoreResultsInRegion() {
		if resp.ScannerId != nil { // ScannerId should be set, but check just in case.
			s.curRegionScannerID = resp.GetScannerId()
		}
		return
	}

	// we are done with this region, prepare scan for next region
	s.curRegionScannerID = noScannerID

	// nil MoreResults means more results
	if resp.MoreResults == nil || *resp.MoreResults {
		s.updateForNextRegion(region)
		return
	}

	// no more results
	s.Close()
}

func (s *scanner) updateForNextRegion(region hrpc.RegionInfo) {
	// Normal Scan
	if !s.rpc.Reversed() {
		stopKey := region.StopKey()
		if len(stopKey) == 0 {
			// StopKey is empty, so it's the last region and the scan is done.
			s.Close()
		} else if len(s.rpc.StopRow()) != 0 && bytes.Compare(s.rpc.StopRow(), stopKey) <= 0 {
			// The request's StopRow has been passed.
			s.Close()
		}
		s.startRow = stopKey
		return
	}

	// Reversed Scan
	startKey := region.StartKey()
	if len(startKey) == 0 {
		// StartKey is empty, so it's the first region and the scan is done.
		s.Close()
		return
	} else if len(s.rpc.StopRow()) != 0 && bytes.Compare(s.rpc.StopRow(), startKey) >= 0 {
		// The request's StopRow has been passed in the reverse direction.
		s.Close()
		return
	}

	// create the nearest value lower than the current region startKey
	// if last element is 0x0, just shorten the slice
	if startKey[len(startKey)-1] == 0x0 {
		s.startRow = startKey[:len(startKey)-1]
		return
	}

	// otherwise lower the last element byte value by 1 and pad with 0xffs
	tmp := make([]byte, len(startKey), len(startKey)+len(rowPadding))
	copy(tmp, startKey)
	tmp[len(tmp)-1]--
	s.startRow = append(tmp, rowPadding...)
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

func (s *scanner) isRegionScannerClosed() bool {
	return s.curRegionScannerID == noScannerID
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
			hrpc.NumberOfRows(0),
			hrpc.ScanStatsID(s.rpc.ScanStatsID()))
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
func (s *scanner) renew(ctx context.Context, scannerID uint64, startRow []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rpc, err := hrpc.NewScanRange(ctx,
		s.rpc.Table(),
		startRow,
		nil,
		hrpc.ScannerID(scannerID),
		hrpc.Priority(s.rpc.Priority()),
		hrpc.RenewalScan(),
		hrpc.ScanStatsID(s.rpc.ScanStatsID()),
	)
	if err != nil {
		return err
	}
	_, err = s.SendRPC(rpc)
	return err
}

func (s *scanner) renewLoop(ctx context.Context, scannerID uint64, startRow []byte) {
	scanRenewers.Inc()
	t := time.NewTicker(s.rpc.RenewInterval())
	defer func() {
		t.Stop()
		scanRenewers.Dec()
	}()

	for {
		select {
		case <-t.C:
			if err := s.renew(ctx, scannerID, startRow); err != nil {
				s.logger.Error("error renewing scanner", "err", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
