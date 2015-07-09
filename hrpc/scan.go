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

func (s *Scan) applyOptions(options ...func(Call) error) (*Scan, error) {
	for _, option := range options {
		err := option(s)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func NewScan(ctx context.Context, table []byte, options ...func(Call) error) (*Scan, error) {
	scan := &Scan{
		base: base{
			table: table,
			ctx:   ctx,
		},
		closeScanner: false,
	}
	return scan.applyOptions(options)
}

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
	return scan.applyOptions(options)
}

func NewScanStr(ctx context.Context, table string, options ...func(Call) error) (*Scan, error) {
	return NewScan(ctx, []byte(table), options...)
}

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

// GetName returns the name of this RPC call.
func (s *Scan) GetName() string {
	return "Scan"
}

func (s *Scan) GetStopRow() []byte {
	return s.stopRow
}

func (s *Scan) GetStartRow() []byte {
	return s.startRow
}

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

func (s *Scan) SetFamilies(fam map[string][]string) error {
	s.families = fam
	return nil
}

func (s *Scan) SetFilter(ft filter.Filter) error {
	s.filters = ft
	return nil
}
