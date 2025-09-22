// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func TestCellFromCellBlock(t *testing.T) {
	cellblock := []byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46}

	cell, n, err := cellFromCellBlock(cellblock)
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

	if !proto.Equal(expectedCell, cell) {
		t.Errorf("expected cell %v, got cell %v", expectedCell, cell)
	}

	// test error cases
	for i := range cellblock {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cell, n, err := cellFromCellBlock(cellblock[:i])
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

	cellblock[3] = 42
	_, _, err = cellFromCellBlock(cellblock)
	expectedErr := "HBase has lied about KeyValue length: expected 42, got 48"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("expected error %q, got error %q", expectedErr, err)
	}
}

func TestDeserializeCellblocks(t *testing.T) {
	cellblocks := []byte{0, 0, 0, 50, 0, 0, 0, 41, 0, 0, 0, 1, 0, 26, 84, 101, 115, 116, 83, 99,
		97, 110, 84, 105, 109, 101, 82, 97, 110, 103, 101, 86, 101, 114, 115, 105, 111, 110, 115,
		49, 2, 99, 102, 97, 0, 0, 0, 0, 0, 0, 0, 51, 4, 49, 0, 0, 0, 50, 0, 0, 0, 41, 0, 0, 0, 1,
		0, 26, 84, 101, 115, 116, 83, 99, 97, 110, 84, 105, 109, 101, 82, 97, 110, 103, 101, 86,
		101, 114, 115, 105, 111, 110, 115, 50, 2, 99, 102, 97, 0, 0, 0, 0, 0, 0, 0, 52, 4, 49}

	cells, read, err := deserializeCellBlocks(cellblocks, 2)
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

	if !reflect.DeepEqual(expectedCells, cells) {
		t.Errorf("expected %v, got %v", expectedCells, cells)
	}

	// test error cases
	cells, _, err = deserializeCellBlocks(cellblocks[:100], 2)
	expectedErr := "buffer is too small: expected 54, got 46"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("expected error %q, got error %q", expectedErr, err)
	}
	if cells != nil {
		t.Errorf("expected no cells, got %v", cells)
	}

	cells, read, err = deserializeCellBlocks(cellblocks, 1)
	if err != nil {
		t.Error(err)
	}
	if expected := expectedCells[:1]; !reflect.DeepEqual(expected, cells) {
		t.Errorf("expected cells %v, got cells %v", expected, cells)
	}
	if int(read) != 54 {
		t.Errorf("invalid number of bytes read: expected %d, got %d", 54, int(read))
	}
}

// BenchmarkDeserializeCellblocks compares shows cost of deserializing
// various sizes of cell blocks into
//  1. pb.Cells (v1)
//  2. CellV2s
//  3. the combined cost of CellV2s + the conversion back to pb.Cells for backwards compatibility.
//
// 2. represents the work of users of client.ScanV2 and 3. represents
// the work needed for users of client.Scan.
func BenchmarkDeserializeCellblocks(b *testing.B) {
	cell := appendCellblock(
		bytes.Repeat([]byte("0123456789"), 5), // 50-byte key
		"f",
		string(bytes.Repeat([]byte("9876543210"), 2)), // 20-byte qualifier
		bytes.Repeat([]byte("abcdefghij"), 20),        // 200-byte value
		17356887651735688765,
		byte(pb.CellType_PUT),
		nil,
	)

	b.Run("oneCell", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocks(cell, 1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("oneCellV2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocksV2(cell, 1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("oneCellV2ToV1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			cellsV2, _, err := deserializeCellBlocksV2(cell, 1)
			if err != nil {
				b.Fatal(err)
			}
			cellsV2toCells(cellsV2)
		}
	})

	cellsTen := bytes.Repeat(cell, 10)
	b.Run("tenCells", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocks(cellsTen, 10)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("tenCellsV2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocksV2(cellsTen, 10)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("tenCellsV2ToV1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			cellsV2, _, err := deserializeCellBlocksV2(cellsTen, 10)
			if err != nil {
				b.Fatal(err)
			}
			cellsV2toCells(cellsV2)
		}
	})

	const twoHunderedKiB = 200 * 1024
	cellCount200KB := twoHunderedKiB / len(cell)
	cells200KB := bytes.Repeat(cell, cellCount200KB)

	b.Run("200KBCells", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocks(cells200KB, uint32(cellCount200KB))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("200KBCellsV2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocksV2(cells200KB, uint32(cellCount200KB))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("200KBCellsV2ToV1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			cellsV2, _, err := deserializeCellBlocksV2(cells200KB, uint32(cellCount200KB))
			if err != nil {
				b.Fatal(err)
			}
			cellsV2toCells(cellsV2)
		}
	})

	const twoMiB = 2 * 1024 * 1024
	cellCount2MB := twoMiB / len(cell)
	cells2MB := bytes.Repeat(cell, cellCount2MB)
	b.Run("2MBCells", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocks(cells2MB, uint32(cellCount2MB))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("2MBCellsV2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, err := deserializeCellBlocksV2(cells2MB, uint32(cellCount2MB))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("2MBCellsV2ToV1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			cellsV2, _, err := deserializeCellBlocksV2(cells2MB, uint32(cellCount2MB))
			if err != nil {
				b.Fatal(err)
			}
			cellsV2toCells(cellsV2)
		}
	})
}
