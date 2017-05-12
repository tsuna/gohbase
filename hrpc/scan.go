// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
)

const (
	// DefaultMaxVersions defualt value for maximum versions to return for scan queries
	DefaultMaxVersions uint32 = 1
	// MinTimestamp default value for minimum timestamp for scan queries
	MinTimestamp uint64 = 0
	// MaxTimestamp default value for maximum timestamp for scan queries
	MaxTimestamp = math.MaxUint64
	// DefaultMaxResultSize Maximum number of bytes fetched when calling a scanner's
	// next method. The default value is 2MB, which is good for 1ge networks.
	// With faster and/or high latency networks this value should be increased.
	DefaultMaxResultSize = 2097152
	// DefaultNumberOfRows is default maximum number of rows fetched by scanner
	DefaultNumberOfRows = math.MaxInt32
)

// Scanner is used to read data sequentially from HBase.
// Scanner will be automatically closed if there's no more data to read,
// otherwise Close method should be called.
type Scanner interface {
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
	Next() (*Result, error)

	// Close should be called if it is desired to stop scanning before getting all of results.
	// If you call Next() after calling Close() you might still get buffered results.
	// Othwerwise, in case all results have been delivered or in case of an error, the Scanner
	// will be closed automatically.
	Close() error
}

// Scan represents a scanner on an HBase table.
type Scan struct {
	base

	// Maps a column family to a list of qualifiers
	families map[string][]string

	startRow []byte
	stopRow  []byte

	fromTimestamp uint64
	toTimestamp   uint64

	maxVersions uint32

	scannerID uint64

	maxResultSize uint64
	numberOfRows  uint32

	filters filter.Filter

	closeScanner        bool
	allowPartialResults bool
}

// baseScan returns a Scan struct with default values set.
func baseScan(ctx context.Context, table []byte,
	options ...func(Call) error) (*Scan, error) {
	s := &Scan{
		base: base{
			table: table,
			ctx:   ctx,
		},
		fromTimestamp: MinTimestamp,
		toTimestamp:   MaxTimestamp,
		maxVersions:   DefaultMaxVersions,
		scannerID:     math.MaxUint64,
		maxResultSize: DefaultMaxResultSize,
		numberOfRows:  DefaultNumberOfRows,
	}
	err := applyOptions(s, options...)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// NewScan creates a scanner for the given table.
func NewScan(ctx context.Context, table []byte, options ...func(Call) error) (*Scan, error) {
	return baseScan(ctx, table, options...)
}

// NewScanRange creates a scanner for the given table and key range.
// The range is half-open, i.e. [startRow; stopRow[ -- stopRow is not
// included in the range.
func NewScanRange(ctx context.Context, table, startRow, stopRow []byte,
	options ...func(Call) error) (*Scan, error) {
	scan, err := baseScan(ctx, table, options...)
	if err != nil {
		return nil, err
	}
	scan.startRow = startRow
	scan.stopRow = stopRow
	scan.key = startRow
	return scan, nil
}

// NewScanStr creates a scanner for the given table.
func NewScanStr(ctx context.Context, table string, options ...func(Call) error) (*Scan, error) {
	return NewScan(ctx, []byte(table), options...)
}

// NewScanRangeStr creates a scanner for the given table and key range.
// The range is half-open, i.e. [startRow; stopRow[ -- stopRow is not
// included in the range.
func NewScanRangeStr(ctx context.Context, table, startRow, stopRow string,
	options ...func(Call) error) (*Scan, error) {
	return NewScanRange(ctx, []byte(table), []byte(startRow), []byte(stopRow), options...)
}

// NewScanFromID creates a new Scan request that will return additional
// results from the given scanner ID.  This is an internal method, users
// are not expected to deal with scanner IDs.
func NewScanFromID(ctx context.Context, table []byte, scannerID uint64, startRow []byte) *Scan {
	scan, _ := baseScan(ctx, table)
	scan.scannerID = scannerID
	scan.key = startRow
	return scan
}

// NewCloseFromID creates a new Scan request that will close the scanner for
// the given scanner ID.  This is an internal method, users are not expected
// to deal with scanner IDs.
func NewCloseFromID(ctx context.Context, table []byte,
	scannerID uint64, startRow []byte) *Scan {
	scan, _ := baseScan(ctx, table)
	scan.scannerID = scannerID
	scan.closeScanner = true
	scan.key = startRow
	return scan
}

func (s *Scan) String() string {
	return fmt.Sprintf("Scan{Table=%q StartRow=%q StopRow=%q TimeRange=(%d, %d) "+
		"MaxVersions=%d NumberOfRows=%d ScannerID=%d Close=%v}",
		s.table, s.startRow, s.stopRow, s.fromTimestamp, s.toTimestamp,
		s.maxVersions, s.numberOfRows, s.scannerID, s.closeScanner)
}

// Name returns the name of this RPC call.
func (s *Scan) Name() string {
	return "Scan"
}

// StopRow returns the end key (exclusive) of this scanner.
func (s *Scan) StopRow() []byte {
	return s.stopRow
}

// StartRow returns the start key (inclusive) of this scanner.
func (s *Scan) StartRow() []byte {
	return s.startRow
}

// IsClosing returns wether this scan closes scanner prematurely
func (s *Scan) IsClosing() bool {
	return s.closeScanner
}

// Families returns the set families covered by this scanner.
// If no families are specified then all the families are scanned.
func (s *Scan) Families() map[string][]string {
	return s.families
}

// Filter returns the filter set on this scanner.
func (s *Scan) Filter() filter.Filter {
	return s.filters
}

// TimeRange returns the to and from timestamps set on this scanner.
func (s *Scan) TimeRange() (uint64, uint64) {
	return s.fromTimestamp, s.toTimestamp
}

// MaxVersions returns the max versions set on this scanner.
func (s *Scan) MaxVersions() uint32 {
	return s.maxVersions
}

// MaxResultSize returns Maximum number of bytes fetched when calling a scanner's next method.
func (s *Scan) MaxResultSize() uint64 {
	return s.maxResultSize
}

// NumberOfRows returns maximum number of rows that will be fetched
// with each scan request to regionserver.
func (s *Scan) NumberOfRows() uint32 {
	return s.numberOfRows
}

// AllowPartialResults returns true if client handles partials.
func (s *Scan) AllowPartialResults() bool {
	return s.allowPartialResults
}

// ToProto converts this Scan into a protobuf message
func (s *Scan) ToProto() (proto.Message, error) {
	scan := &pb.ScanRequest{
		Region:       s.regionSpecifier(),
		CloseScanner: &s.closeScanner,
		NumberOfRows: proto.Uint32(math.MaxInt32),
		// tell server that we can process results that are only part of a row
		ClientHandlesPartials: proto.Bool(true),
		// tell server that we "handle" heartbeats by ignoring them
		// since we don't really time out our scans (unless context was cancelled)
		ClientHandlesHeartbeats: proto.Bool(true),
	}
	if s.scannerID != math.MaxUint64 {
		scan.ScannerId = &s.scannerID
		return scan, nil
	}
	scan.Scan = &pb.Scan{
		Column:        familiesToColumn(s.families),
		StartRow:      s.startRow,
		StopRow:       s.stopRow,
		TimeRange:     &pb.TimeRange{},
		MaxResultSize: &s.maxResultSize,
	}
	if s.maxVersions != DefaultMaxVersions {
		scan.Scan.MaxVersions = &s.maxVersions
	}
	if s.fromTimestamp != MinTimestamp {
		scan.Scan.TimeRange.From = &s.fromTimestamp
	}
	if s.toTimestamp != MaxTimestamp {
		scan.Scan.TimeRange.To = &s.toTimestamp
	}

	if s.filters != nil {
		pbFilter, err := s.filters.ConstructPBFilter()
		if err != nil {
			return nil, err
		}
		scan.Scan.Filter = pbFilter
	}
	return scan, nil
}

// NewResponse creates an empty protobuf message to read the response
// of this RPC.
func (s *Scan) NewResponse() proto.Message {
	return &pb.ScanResponse{}
}

// DeserializeCellBlocks deserializes scan results from cell blocks
func (s *Scan) DeserializeCellBlocks(m proto.Message, r io.Reader, cellsLen uint32) error {
	if cellsLen == 0 {
		// if no cell blocks, our job is done since protobuf took care of deserialization
		return nil
	}

	cells, err := deserializeCellBlocks(r, cellsLen)
	if err != nil {
		return err
	}

	scanResp := m.(*pb.ScanResponse)
	partials := scanResp.GetPartialFlagPerResult()
	for i, numCells := range scanResp.GetCellsPerResult() {
		scanResp.Results = append(scanResp.Results, &pb.Result{
			Cell:    cells[:numCells],
			Partial: proto.Bool(partials[i]),
		})
		cells = cells[numCells:]
	}
	return nil
}

// SetFamilies sets the families covered by this scanner.
func (s *Scan) SetFamilies(fam map[string][]string) error {
	s.families = fam
	return nil
}

// SetFilter sets the request's filter.
func (s *Scan) SetFilter(ft filter.Filter) error {
	s.filters = ft
	return nil
}

// MaxResultSize is an option for scan requests.
// Maximum number of bytes fetched when calling a scanner's next method.
// MaxResultSize takes priority over NumberOfRows.
func MaxResultSize(n uint64) func(Call) error {
	return func(g Call) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("'MaxResultSize' option can only be used with Scan queries")
		}
		if n == 0 {
			return errors.New("'MaxResultSize' option must be greater than 0")
		}
		scan.maxResultSize = n
		return nil
	}
}

// NumberOfRows is an option for scan requests.
// Specifies how many rows are fetched with each request to regionserver.
// Should be > 0, avoid extremely low values such as 1 because a request
// to regionserver will be made for every row.
func NumberOfRows(n uint32) func(Call) error {
	return func(g Call) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("'NumberOfRows' option can only be used with Scan queries")
		}
		if n == 0 {
			return errors.New("'NumberOfRows' option must be greater than 0")
		}
		scan.numberOfRows = n
		return nil
	}
}

// AllowPartialResults is an option for scan requests.
// This option should be provided if the client has really big rows and
// wants to avoid OOM errors on her side. With this option provided, Next()
// will return partial rows.
func AllowPartialResults() func(Call) error {
	return func(g Call) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("'AllowPartialResults' option can only be used with Scan queries")
		}
		scan.allowPartialResults = true
		return nil
	}
}
