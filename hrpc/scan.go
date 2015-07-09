// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

// Scan represents a scanner on an HBase table.
type Scan struct {
	base

	// Maps a column family to a list of qualifiers
	families map[string][]string

	closeScanner bool

	startRow []byte
	stopRow  []byte

	scannerID *uint64

	filters filter.Filter
}

// NewScan is called to construct a Scan* object which is then passed as the sole parameter for a
// client.Scan(). Uses functional options for arguments, see
// http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis for more information.
//
// Allows usage like the following -
//
//	scan, err := hrpc.NewScan(ctx, table)
//	scan, err := hrpc.NewScan(ctx, table, hrpc.Families(fam))
//	scan, err := hrpc.NewScan(ctx, table, hrpc.Filters(filter))
//	scan, err := hrpc.NewScan(ctx, table, hrpc.Families(fam), hrpc.Filters(filter))
//
func NewScan(ctx context.Context, table []byte, options ...func(Call) error) (*Scan, error) {
	scan := &Scan{
		base: base{
			table: table,
			ctx:   ctx,
		},
		closeScanner: false,
	}
	return scan.applyOptions(options...)
}

// NewScanRange is equivalent to NewScan but adds two additional parameters - startRow, stopRow.
// This allows a range to be scanned without having to go through the overhead of using a RowFilter
func NewScanRange(ctx context.Context, table, startRow, stopRow []byte,
	options ...func(Call) error) (*Scan, error) {
	scan := &Scan{
		base: base{
			table: table,
			key:   stopRow,
			ctx:   ctx,
		},
		closeScanner: false,
		startRow:     startRow,
		stopRow:      stopRow,
	}
	return scan.applyOptions(options...)
}

// NewScanStr wraps NewScan but allows the table to be specified as a string.
func NewScanStr(ctx context.Context, table string, options ...func(Call) error) (*Scan, error) {
	return NewScan(ctx, []byte(table), options...)
}

// NewScanRangeStr wraps NewScanRange but allows table, startRow, stopRow to be specified as strings
func NewScanRangeStr(ctx context.Context, table, startRow, stopRow string,
	options ...func(Call) error) (*Scan, error) {
	return NewScanRange(ctx, []byte(table), []byte(startRow), []byte(stopRow), options...)
}

// NewScanFromID creates a new Scan request that will return additional results
// from a given scanner ID.
func NewScanFromID(ctx context.Context, table []byte, scannerID uint64, startRow []byte) *Scan {
	return &Scan{
		base: base{
			table: []byte(table),
			key:   []byte(startRow),
			ctx:   ctx,
		},
		scannerID:    &scannerID,
		closeScanner: false,
	}
}

// NewCloseFromID creates a new Scan request that will close the scan for a
// given scanner ID.
func NewCloseFromID(ctx context.Context, table []byte, scannerID uint64, startRow []byte) *Scan {
	return &Scan{
		base: base{
			table: []byte(table),
			key:   []byte(startRow),
			ctx:   ctx,
		},
		scannerID:    &scannerID,
		closeScanner: true,
	}
}

func (s *Scan) applyOptions(options ...func(Call) error) (*Scan, error) {
	for _, option := range options {
		err := option(s)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// GetName returns the name of this RPC call.
func (s *Scan) GetName() string {
	return "Scan"
}

// GetStopRow returns the set stopRow.
func (s *Scan) GetStopRow() []byte {
	return s.stopRow
}

// GetStartRow returns the set startRow.
func (s *Scan) GetStartRow() []byte {
	return s.startRow
}

// GetFamilies returns the set families.
func (s *Scan) GetFamilies() map[string][]string {
	return s.families
}

// GetRegionStop returns the stop key of the region currently being scanned.
func (s *Scan) GetRegionStop() []byte {
	return s.region.StopKey
}

// Serialize will convert this Scan into a serialized protobuf message ready
// to be sent to an HBase node.
func (s *Scan) Serialize() ([]byte, error) {
	scan := &pb.ScanRequest{
		Region:       s.regionSpecifier(),
		CloseScanner: &s.closeScanner,
		NumberOfRows: proto.Uint32(20), //TODO: make this configurable
	}
	if s.scannerID == nil {
		scan.Scan = &pb.Scan{
			Column:   familiesToColumn(s.families),
			StartRow: s.startRow,
			StopRow:  s.stopRow,
		}
	} else {
		scan.ScannerId = s.scannerID
	}
	return proto.Marshal(scan)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (s *Scan) NewResponse() proto.Message {
	return &pb.ScanResponse{}
}

// SetFamilies sets the request's families.
func (s *Scan) SetFamilies(fam map[string][]string) error {
	s.families = fam
	return nil
}

// SetFilter sets the request's filter.
func (s *Scan) SetFilter(ft filter.Filter) error {
	s.filters = ft
	return nil
}
