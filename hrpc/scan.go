// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
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
func NewScanStr(table string, families map[string][]string, startRow, stopRow []byte) *Scan {
	scan := &Scan{
		base: base{
			table: []byte(table),
			key:   startRow,
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
func NewScanFromID(table string, scannerID uint64, startRow []byte) *Scan {
	return &Scan{
		base: base{
			table: []byte(table),
			key:   []byte(startRow),
		},
		scannerID:    &scannerID,
		closeScanner: false,
	}
}

// NewCloseFromID creates a new Scan request that will close the scan for a
// given scanner ID.
func NewCloseFromID(table string, scannerID uint64) *Scan {
	return &Scan{
		base: base{
			table: []byte(table),
			//key:
		},
		scannerID:    &scannerID,
		closeScanner: true,
	}
}

// Name returns the name of this RPC call.
func (s *Scan) Name() string {
	return "Scan"
}

// Serialize will convert this Scan into a serialized protobuf message ready
// to be sent to an HBase node.
func (s *Scan) Serialize() ([]byte, error) {
	x := uint32(20)
	scan := &pb.ScanRequest{
		Region:       s.regionSpecifier(),
		CloseScanner: &s.closeScanner,
		NumberOfRows: &x, //TODO: make this configurable
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
