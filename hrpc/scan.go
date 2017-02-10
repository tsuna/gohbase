// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/internal/pb"
	"golang.org/x/net/context"
)

const (
	// DefaultMaxVersions defualt value for maximum versions to return for scan queries
	DefaultMaxVersions uint32 = 1
	// MinTimestamp default value for minimum timestamp for scan queries
	MinTimestamp uint64 = 0
	// MaxTimestamp default value for maximum timestamp for scan queries
	MaxTimestamp = math.MaxUint64
	// DefaultNumberOfRows is default maximum number of rows fetched by scanner
	DefaultNumberOfRows = 128
)

// Scan represents a scanner on an HBase table.
type Scan struct {
	base

	// Maps a column family to a list of qualifiers
	families map[string][]string

	closeScanner bool

	startRow []byte
	stopRow  []byte

	fromTimestamp uint64
	toTimestamp   uint64

	maxVersions uint32

	scannerID uint64

	numberOfRows uint32

	filters filter.Filter
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
func NewScanFromID(ctx context.Context, table []byte,
	scannerID uint64, startRow []byte) *Scan {
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

// Families returns the set families covered by this scanner.
// If no families are specified then all the families are scanned.
func (s *Scan) Families() map[string][]string {
	return s.families
}

// RegionStop returns the stop key of the region currently being scanned.
// This is an internal method, end users are not expected to use it.
func (s *Scan) RegionStop() []byte {
	return s.region.StopKey()
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

// NumberOfRows returns maximum number of rows that could be fetched
// by this scanner.
func (s *Scan) NumberOfRows() uint32 {
	return s.numberOfRows
}

// Serialize converts this Scan into a serialized protobuf message ready
// to be sent to an HBase node.
func (s *Scan) Serialize() ([]byte, error) {
	scan := &pb.ScanRequest{
		Region:       s.regionSpecifier(),
		CloseScanner: &s.closeScanner,
		NumberOfRows: &s.numberOfRows,
	}
	if s.scannerID != math.MaxUint64 {
		scan.ScannerId = &s.scannerID
		return proto.Marshal(scan)
	}
	scan.Scan = &pb.Scan{
		Column:    familiesToColumn(s.families),
		StartRow:  s.startRow,
		StopRow:   s.stopRow,
		TimeRange: &pb.TimeRange{},
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
	return proto.Marshal(scan)
}

// NewResponse creates an empty protobuf message to read the response
// of this RPC.
func (s *Scan) NewResponse() proto.Message {
	return &pb.ScanResponse{}
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
