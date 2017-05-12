// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/aristanetworks/goarista/test"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

func TestCellFromCellBlock(t *testing.T) {
	cellblock := []byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46}

	cell, n, err := cellFromCellBlock(bytes.NewBuffer(cellblock))
	if err != nil {
		t.Error(err)
	}

	if int(n) != len(cellblock) {
		t.Errorf("expected %d bytes read, got %d bytes read", len(cellblock), n)
	}

	expectedCell := &pb.Cell{
		Row:       []byte("row7"),
		Family:    []byte("cf"),
		Qualifier: []byte("a"),
		Timestamp: proto.Uint64(1494873081120),
		Value:     []byte("Hello my name is Dog."),
		CellType:  pb.CellType_PUT.Enum(),
	}

	if d := test.Diff(expectedCell, cell); len(d) != 0 {
		t.Error(d)
	}

	// test error cases
	for i := range cellblock {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cell, n, err := cellFromCellBlock(bytes.NewBuffer(cellblock[:i]))
			if err == nil {
				t.Error("expected error, got none")
			}

			if int(n) != 0 {
				t.Errorf("expected %d bytes read, got %d bytes read", 0, n)
			}

			if cell != nil {
				t.Errorf("unexpected cell: %v", cell)
			}
		})
	}

	expectedError := errors.New("HBase has lied about KeyValue length: expected 42, got 48")
	cellblock[3] = 42
	_, _, err = cellFromCellBlock(bytes.NewBuffer(cellblock))
	if d := test.Diff(expectedError, err); len(d) != 0 {
		t.Error(d)
	}

}

func TestDeserializeCellblocks(t *testing.T) {
	cellblocks := []byte{0, 0, 0, 50, 0, 0, 0, 41, 0, 0, 0, 1, 0, 26, 84, 101, 115, 116, 83, 99,
		97, 110, 84, 105, 109, 101, 82, 97, 110, 103, 101, 86, 101, 114, 115, 105, 111, 110, 115,
		49, 2, 99, 102, 97, 0, 0, 0, 0, 0, 0, 0, 51, 4, 49, 0, 0, 0, 50, 0, 0, 0, 41, 0, 0, 0, 1,
		0, 26, 84, 101, 115, 116, 83, 99, 97, 110, 84, 105, 109, 101, 82, 97, 110, 103, 101, 86,
		101, 114, 115, 105, 111, 110, 115, 50, 2, 99, 102, 97, 0, 0, 0, 0, 0, 0, 0, 52, 4, 49}

	cells, err := deserializeCellBlocks(bytes.NewBuffer(cellblocks), uint32(len(cellblocks)))
	if err != nil {
		t.Error(err)
	}

	expectedCells := []*pb.Cell{
		&pb.Cell{
			Row:       []byte("TestScanTimeRangeVersions1"),
			Family:    []byte("cf"),
			Qualifier: []byte("a"),
			Timestamp: proto.Uint64(51),
			Value:     []byte("1"),
			CellType:  pb.CellType_PUT.Enum(),
		},
		&pb.Cell{
			Row:       []byte("TestScanTimeRangeVersions2"),
			Family:    []byte("cf"),
			Qualifier: []byte("a"),
			Timestamp: proto.Uint64(52),
			Value:     []byte("1"),
			CellType:  pb.CellType_PUT.Enum(),
		},
	}

	if d := test.Diff(expectedCells, cells); len(d) != 0 {
		t.Error(d)
	}

	// test error cases
	cells, err = deserializeCellBlocks(bytes.NewBuffer(cellblocks[:100]), uint32(len(cellblocks)))
	expectedError := errors.New("failed to read timestamp: unexpected EOF")
	if d := test.Diff(expectedError, err); len(d) != 0 {
		t.Error(d)
	}
	if d := test.Diff([]*pb.Cell(nil), cells); len(d) != 0 {
		t.Error(d)
	}

	_, err = deserializeCellBlocks(bytes.NewBuffer(cellblocks), uint32(len(cellblocks))-1)
	expectedError = errors.New(
		"HBase has lied about the length of cell blocks: expected 107, read 108")
	if d := test.Diff(expectedError, err); len(d) != 0 {
		t.Error(d)
	}
}
