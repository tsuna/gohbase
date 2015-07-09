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
}

// NewScanStr creates a new Scan request that will start a scan for rows between
// startRow (inclusive) and stopRow (exclusive). The values in the specified
// column families and qualifiers will be returned. When run, a scanner ID will
// also be returned, that can be used to fetch addition results via successive
// Scan requests.
func NewScanStr(ctx context.Context, table string, families map[string][]string, startRow, stopRow []byte) *Scan {
	scan := &Scan{
		base: base{
			table: []byte(table),
			key:   startRow,
			ctx:   ctx,
		},
		families:     families,
		startRow:     startRow,
		stopRow:      stopRow,
		closeScanner: false,
	}
	return scan
}

// NewScanFromID creates a new Scan request that will return additional results
// from a given scanner ID.
func NewScanFromID(ctx context.Context, table string, scannerID uint64, startRow []byte) *Scan {
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
func NewCloseFromID(ctx context.Context, table string, scannerID uint64, startRow []byte) *Scan {
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

// Name returns the name of this RPC call.
func (s *Scan) Name() string {
	return "Scan"
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
	// not implemented yet
	return nil
}
