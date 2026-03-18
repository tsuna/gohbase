// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"testing"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// Benchmark reading cell fields using getters (opaque API)
func BenchmarkCellGetters(b *testing.B) {
	cell := &pb.Cell{}
	cell.SetRow([]byte("row123"))
	cell.SetFamily([]byte("cf"))
	cell.SetQualifier([]byte("qual"))
	cell.SetValue([]byte("value123"))
	cell.SetTimestamp(12345678)
	cell.SetCellType(pb.CellType_PUT)

	b.ResetTimer()
	for b.Loop() {
		_ = cell.GetRow()
		_ = cell.GetFamily()
		_ = cell.GetQualifier()
		_ = cell.GetValue()
		_ = cell.GetTimestamp()
		_ = cell.GetCellType()
	}
}

// Benchmark creating cells using setters (opaque API)
func BenchmarkCellSetters(b *testing.B) {
	row := []byte("row123")
	family := []byte("cf")
	qualifier := []byte("qual")
	value := []byte("value123")
	timestamp := uint64(12345678)
	cellType := pb.CellType_PUT

	b.ResetTimer()
	for b.Loop() {
		cell := &pb.Cell{}
		cell.SetRow(row)
		cell.SetFamily(family)
		cell.SetQualifier(qualifier)
		cell.SetValue(value)
		cell.SetTimestamp(timestamp)
		cell.SetCellType(cellType)
	}
}

// Benchmark marshaling cells
func BenchmarkCellMarshal(b *testing.B) {
	cell := &pb.Cell{}
	cell.SetRow([]byte("row123"))
	cell.SetFamily([]byte("cf"))
	cell.SetQualifier([]byte("qual"))
	cell.SetValue([]byte("value123"))
	cell.SetTimestamp(12345678)
	cell.SetCellType(pb.CellType_PUT)

	b.ResetTimer()
	for b.Loop() {
		_, err := proto.Marshal(cell)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark unmarshaling cells
func BenchmarkCellUnmarshal(b *testing.B) {
	cell := &pb.Cell{}
	cell.SetRow([]byte("row123"))
	cell.SetFamily([]byte("cf"))
	cell.SetQualifier([]byte("qual"))
	cell.SetValue([]byte("value123"))
	cell.SetTimestamp(12345678)
	cell.SetCellType(pb.CellType_PUT)

	data, err := proto.Marshal(cell)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		cell := &pb.Cell{}
		err := proto.Unmarshal(data, cell)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark typical read pattern: unmarshal + read all fields
func BenchmarkCellReadPattern(b *testing.B) {
	cell := &pb.Cell{}
	cell.SetRow([]byte("row123"))
	cell.SetFamily([]byte("cf"))
	cell.SetQualifier([]byte("qual"))
	cell.SetValue([]byte("value123"))
	cell.SetTimestamp(12345678)
	cell.SetCellType(pb.CellType_PUT)

	data, err := proto.Marshal(cell)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		cell := &pb.Cell{}
		if err := proto.Unmarshal(data, cell); err != nil {
			b.Fatal(err)
		}
		_ = cell.GetRow()
		_ = cell.GetFamily()
		_ = cell.GetQualifier()
		_ = cell.GetValue()
		_ = cell.GetTimestamp()
		_ = cell.GetCellType()
	}
}

// Benchmark memory allocations for cell creation
func BenchmarkCellAllocation(b *testing.B) {
	b.ReportAllocs()
	row := []byte("row123")
	family := []byte("cf")
	qualifier := []byte("qual")
	value := []byte("value123")

	b.ResetTimer()
	for b.Loop() {
		cell := &pb.Cell{}
		cell.SetRow(row)
		cell.SetFamily(family)
		cell.SetQualifier(qualifier)
		cell.SetValue(value)
		cell.SetTimestamp(12345678)
		cell.SetCellType(pb.CellType_PUT)
	}
}

// Benchmark that prevents compiler optimization by using the result
func BenchmarkCellGettersNoOptimize(b *testing.B) {
	cell := &pb.Cell{}
	cell.SetRow([]byte("row123"))
	cell.SetFamily([]byte("cf"))
	cell.SetQualifier([]byte("qual"))
	cell.SetValue([]byte("value123"))
	cell.SetTimestamp(12345678)
	cell.SetCellType(pb.CellType_PUT)

	var sink []byte
	b.ResetTimer()
	for b.Loop() {
		sink = cell.GetRow()
		sink = cell.GetFamily()
		sink = cell.GetQualifier()
		sink = cell.GetValue()
	}
	_ = sink
}

// Benchmark realistic usage: create cell, set fields, read them back
func BenchmarkCellRoundTrip(b *testing.B) {
	row := []byte("row123")
	family := []byte("cf")
	qualifier := []byte("qual")
	value := []byte("value123")

	var sink []byte
	b.ResetTimer()
	for b.Loop() {
		cell := &pb.Cell{}
		cell.SetRow(row)
		cell.SetFamily(family)
		cell.SetQualifier(qualifier)
		cell.SetValue(value)
		cell.SetTimestamp(12345678)
		cell.SetCellType(pb.CellType_PUT)

		sink = cell.GetRow()
		sink = cell.GetValue()
	}
	_ = sink
}

// Benchmark creating many cells to check allocation patterns
func BenchmarkManyCellsAllocation(b *testing.B) {
	b.ReportAllocs()
	row := []byte("row123")
	family := []byte("cf")
	qualifier := []byte("qual")
	value := []byte("value123")

	b.ResetTimer()
	for b.Loop() {
		cells := make([]*pb.Cell, 100)
		for i := 0; i < 100; i++ {
			cell := &pb.Cell{}
			cell.SetRow(row)
			cell.SetFamily(family)
			cell.SetQualifier(qualifier)
			cell.SetValue(value)
			cell.SetTimestamp(12345678)
			cell.SetCellType(pb.CellType_PUT)
			cells[i] = cell
		}
	}
}

// Benchmark slice operations with cells
func BenchmarkCellSliceOperations(b *testing.B) {
	b.ReportAllocs()
	cells := make([]*pb.Cell, 1000)
	for i := range cells {
		cell := &pb.Cell{}
		cell.SetRow([]byte("row"))
		cell.SetFamily([]byte("cf"))
		cell.SetQualifier([]byte("qual"))
		cell.SetValue([]byte("value"))
		cell.SetTimestamp(uint64(i))
		cell.SetCellType(pb.CellType_PUT)
		cells[i] = cell
	}

	b.ResetTimer()
	for b.Loop() {
		var sum uint64
		for _, cell := range cells {
			sum += cell.GetTimestamp()
		}
		_ = sum
	}
}

// Benchmark copying cell data
func BenchmarkCellCopy(b *testing.B) {
	b.ReportAllocs()
	src := &pb.Cell{}
	src.SetRow([]byte("row123"))
	src.SetFamily([]byte("cf"))
	src.SetQualifier([]byte("qual"))
	src.SetValue([]byte("value123"))
	src.SetTimestamp(12345678)
	src.SetCellType(pb.CellType_PUT)

	cells := make([]*pb.Cell, 1000)

	b.ResetTimer()
	for b.Loop() {
		dst := &pb.Cell{}
		dst.SetRow(src.GetRow())
		dst.SetFamily(src.GetFamily())
		dst.SetQualifier(src.GetQualifier())
		dst.SetValue(src.GetValue())
		dst.SetTimestamp(src.GetTimestamp())
		dst.SetCellType(src.GetCellType())
		cells = append(cells, dst)
	}
}
