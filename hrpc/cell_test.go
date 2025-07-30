// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"slices"
	"strconv"
	"testing"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func TestCellV2FromCellBlock(t *testing.T) {
	cellblock := []byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46}

	cell, n, err := cellV2FromCellBlock(cellblock)
	if err != nil {
		t.Error(err)
	}

	if int(n) != len(cellblock) {
		t.Errorf("expected %d bytes read, got %d bytes read", len(cellblock), n)
	}

	cell2, err := NewCellV2(cell.Row(), cell.Family(), cell.Qualifier(),
		cell.Timestamp(), cell.Value(), cell.CellType())
	if err != nil {
		t.Fatal(err)
	}
	if !cell.Equal(cell2) {
		t.Errorf("expected cell2 to equal cell, but they are not: cell: %s cell2: %s", cell, cell2)
	}

	expectedCell := &pb.Cell{
		Row:       []byte("row7"),
		Family:    []byte("cf"),
		Qualifier: []byte("a"),
		Timestamp: proto.Uint64(1494873081120),
		Value:     []byte("Hello my name is Dog."),
		CellType:  pb.CellType_PUT.Enum(),
	}

	if !proto.Equal(expectedCell, cell.ToPBCell()) {
		t.Errorf("expected cell %v, got cell %v", expectedCell, cell.ToPBCell())
	}

	// test error cases
	for i := range cellblock {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cell, n, err := cellV2FromCellBlock(cellblock[:i])
			if err == nil {
				t.Error("expected error, got none")
			}

			if int(n) != 0 {
				t.Errorf("expected %d bytes read, got %d bytes read", 0, n)
			}

			if cell.b != nil {
				t.Errorf("unexpected cell: %v", cell)
			}
		})
	}

	cellblock[3] = 42
	_, _, err = cellV2FromCellBlock(cellblock)
	expectedErr := "invalid cell: buffer too small, expected at least 21 bytes"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("expected error %q, got error %q", expectedErr, err)
	}
}

func TestDeserializeCellblocksV2(t *testing.T) {
	cellblocks := []byte{0, 0, 0, 50, 0, 0, 0, 41, 0, 0, 0, 1, 0, 26, 84, 101, 115, 116, 83, 99,
		97, 110, 84, 105, 109, 101, 82, 97, 110, 103, 101, 86, 101, 114, 115, 105, 111, 110, 115,
		49, 2, 99, 102, 97, 0, 0, 0, 0, 0, 0, 0, 51, 4, 49, 0, 0, 0, 50, 0, 0, 0, 41, 0, 0, 0, 1,
		0, 26, 84, 101, 115, 116, 83, 99, 97, 110, 84, 105, 109, 101, 82, 97, 110, 103, 101, 86,
		101, 114, 115, 105, 111, 110, 115, 50, 2, 99, 102, 97, 0, 0, 0, 0, 0, 0, 0, 52, 4, 49}

	cells, read, err := deserializeCellBlocksV2(cellblocks, 2)
	if err != nil {
		t.Error(err)
	}
	if int(read) != len(cellblocks) {
		t.Errorf("invalid number of bytes read: expected %d, got %d", len(cellblocks), int(read))
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

	if !slices.EqualFunc(expectedCells, cells, func(a *pb.Cell, b CellV2) bool {
		return proto.Equal(a, b.ToPBCell())
	}) {
		t.Errorf("expected %v, got %v", expectedCells, cells)
	}

	// test error cases
	cells, _, err = deserializeCellBlocksV2(cellblocks[:100], 2)
	expectedErr := "invalid cell: buffer too small, expected at least 54 bytes"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("expected error %q, got error %q", expectedErr, err)
	}
	if cells != nil {
		t.Errorf("expected no cells, got %v", cells)
	}

	cells, read, err = deserializeCellBlocksV2(cellblocks, 1)
	if err != nil {
		t.Error(err)
	}
	expected := expectedCells[:1]
	if !slices.EqualFunc(expected, cells, func(a *pb.Cell, b CellV2) bool {
		return proto.Equal(a, b.ToPBCell())
	}) {
		t.Errorf("expected %v, got %v", expectedCells, cells)
	}
	if int(read) != 54 {
		t.Errorf("invalid number of bytes read: expected %d, got %d", 54, int(read))
	}
}

func FuzzCellV2FromCellBlock(f *testing.F) {
	f.Add([]byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46})
	f.Fuzz(func(t *testing.T, b []byte) {
		cell, _, err := cellV2FromCellBlock(b)
		if err != nil {
			return
		}
		// if this was a successfully parsed cell then verify we can
		// re-serialize it.
		cell2, err := NewCellV2(cell.Row(), cell.Family(), cell.Qualifier(), cell.Timestamp(),
			cell.Value(), cell.CellType())
		if err != nil {
			t.Fatal(err)
		}
		if !cell.Equal(cell2) {
			t.Errorf("parsed cell does not serialize the same: %v", cell.b)
		}
	})
}
