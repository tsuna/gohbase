// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

/*
import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

// Get represents a Get HBase call.
type Scan struct {
	base

	families map[string][]string //Maps a column family to a list of qualifiers

	closeScanner bool

	startRow []byte
	stopRow  []byte

	scannerId *uint64
}

// NewScanStr creates a new Scan request that will start a scan for rows between
// startRow (inclusive) and stopRow (exclusive). The values in the specified
// column families and qualifiers will be returned. When run, a scanner Id will
// also be returned, that can be used to fetch addition results via successive
// Scan requests.
func NewScanStr(table string, families map[string][]string, startRow, stoprow *[]byte) *Scan {
	return &Scan{
		base: base{
			table: []byte(table),
			//key:
		},
		families:     families,
		startRow:     startRow,
		stopRow:      stopRow,
		closeScanner: false,
	}
}

// NewScanFromId creates a new Scan request that will return additional results
// from a given scanner Id
func NewScanFromId(table string, scannerId uint64) {
	return &Scan{
		base: base{
			table: []byte(table),
			//key:
		},
		scannerId:    &scannerId,
		closeScanner: false,
	}
}

// NewScanCloseId creates a new Scan request that will close the scan for a
// given scanner Id
func NewScanCloseId(table string, scannerId uint64) {
	return &Scan{
		base: base{
			table: []byte(table),
			//key:
		},
		scannerId:    &scannerId,
		closeScanner: true,
	}
}

// Name returns the name of this RPC call.
func (s *Scan) Name() string {
	return "Scan"
}

func (s *Scan) Serialize() ([]byte, error) {
	scan := &pb.ScanRequest{
		Region:       s.regionSpecifier(),
		CloseScanner: s.closeScanner,
	}
	if scannerId == nil {
		scan.Scan = &pb.Scan{
			Column: familiesToColumn(s.families),
		}
		if s.startRow != nil {
			scan.Scan.StartRow = *s.startRow
		}
		if s.stopRow != nil {
			scan.Scan.StopRow = *s.StopRow
		}
	} else {
		scan.ScannerId = s.scannerId
	}
	return proto.Marshal(scan)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (s *Scan) NewResponse() proto.Message {
	return &pb.ScanResponse{}
}
*/
