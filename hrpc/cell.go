// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/tsuna/gohbase/pb"
)

// CellV2 represents a single cell of data.
type CellV2 struct {
	// This struct is optimized for size by limiting the number of
	// slices to just one and limiting the amount of data to a
	// minimum. All data is accessed through methods that operate on
	// offsets into b.
	b            []byte
	qualifierLen uint32
	rowLen       uint16
	familyLen    uint8
}

// cellV2FromCellBlock parses and verifies a CellV2 from a byte slice
// containing 1 or more cells. It returns the CellV2, the number of
// bytes consumed on success or an error if the cell was invalid.
//
// cellblock layout:
//
// Header:
// 4 byte length of key + value
// 4 byte length of key
// 4 byte length of value
//
// Key:
// 2 byte length of row
// <row>
// 1 byte length of row family
// <family>
// <qualifier>
// 8 byte timestamp
// 1 byte type
//
// Value:
// <value>
func cellV2FromCellBlock(b []byte) (CellV2, uint32, error) {
	var offset int

	kvLen, i, err := u32(b)
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	// Clip b to just the size of this cell. This prevents reading
	// past the end of the expected size of the slice.
	b, _, err = byts(b, 4+int(kvLen))
	if err != nil {
		return CellV2{}, 0, err
	}
	b = slices.Clip(b)

	keyLen, i, err := u32(b[offset:])
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	valueLen, i, err := u32(b[offset:])
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	rowLen, i, err := u16(b[offset:])
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	// read past row
	_, i, err = byts(b[offset:], int(rowLen))
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	familyLen, i, err := byt(b[offset:])
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	qualifierLen := int(keyLen) -
		(2 /* sizeof(rowLen) */ + int(rowLen) +
			1 /* sizeof(familyLen) */ + int(familyLen) +
			8 /* timestamp */ + 1 /* celltype */)
	if qualifierLen < 0 {
		return CellV2{}, 0, errors.New("invalid cell")
	}

	// Note: all the following code is just verification of the cell.

	// read past family
	_, i, err = byts(b[offset:], int(familyLen))
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	// read past qualifier
	_, i, err = byts(b[offset:], qualifierLen)
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	// read past timestamp
	_, i, err = u64(b[offset:])
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	// read past type
	_, i, err = byt(b[offset:])
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	// read past value
	_, i, err = byts(b[offset:], int(valueLen))
	if err != nil {
		return CellV2{}, 0, err
	}
	offset += i

	if offset != len(b) {
		return CellV2{}, 0, errors.New("invalid cell: didn't consume the entire cell")
	}

	c := CellV2{b: b, rowLen: rowLen, familyLen: familyLen, qualifierLen: uint32(qualifierLen)}
	return c, uint32(len(b)), nil
}

func byt(b []byte) (byte, int, error) {
	if len(b) < 1 {
		return 0, 0, errors.New("invalid cell: buffer too small to parse byte")
	}
	return b[0], 1, nil
}

func byts(b []byte, l int) ([]byte, int, error) {
	if len(b) < l {
		return nil, 0, fmt.Errorf("invalid cell: buffer too small, expected at least %d bytes", l)
	}
	return b[:l], l, nil
}

func u16(b []byte) (uint16, int, error) {
	if len(b) < 2 {
		return 0, 0, errors.New("invalid cell: buffer too small to parse uint16")
	}
	return binary.BigEndian.Uint16(b), 2, nil
}

func u32(b []byte) (uint32, int, error) {
	if len(b) < 4 {
		return 0, 0, errors.New("invalid cell: buffer too small to parse uint32")
	}
	return binary.BigEndian.Uint32(b), 4, nil
}

func u64(b []byte) (uint64, int, error) {
	if len(b) < 8 {
		return 0, 0, errors.New("invalid cell: buffer too small to parse uint64")
	}
	return binary.BigEndian.Uint64(b), 8, nil
}

// NewCellV2 creates a new CellV2 from fields
func NewCellV2(row, family, qualifier []byte, timestamp uint64, value []byte,
	celltype pb.CellType) (CellV2, error) {
	if len(row) > math.MaxUint16 {
		return CellV2{}, fmt.Errorf("row length cannot fit in a uint16: %d", len(row))
	}
	if len(family) > math.MaxUint8 {
		return CellV2{}, fmt.Errorf("family length cannot fit in a uint8: %d", len(family))
	}
	if uint64(len(qualifier)) > math.MaxUint32 {
		return CellV2{}, fmt.Errorf("qualifier length cannot fit in a uint32: %d", len(qualifier))
	}
	if uint64(len(value)) > math.MaxUint32 {
		return CellV2{}, fmt.Errorf("value length cannot fit in a uint32: %d", len(value))
	}

	c := CellV2{
		qualifierLen: uint32(len(qualifier)),
		rowLen:       uint16(len(row)),
		familyLen:    uint8(len(family)),
	}

	// The layout is documented in the cellV2FromCellBlock function.
	rowKeyLen := 2 + uint32(c.rowLen) + 1 + uint32(c.familyLen) + uint32(c.qualifierLen) + 8 + 1
	kvLen := 4 + 4 + rowKeyLen + uint32(len(value)) // keyLen and len(value) fields + key + value

	totalLen := 4 + kvLen // kvLen field + the rest

	c.b = make([]byte, totalLen)

	// Header
	binary.BigEndian.PutUint32(c.b[0:4], kvLen)
	binary.BigEndian.PutUint32(c.b[4:8], rowKeyLen)
	binary.BigEndian.PutUint32(c.b[8:12], uint32(len(value)))

	offset := 12

	binary.BigEndian.PutUint16(c.b[offset:offset+2], uint16(len(row)))
	offset += 2
	offset += copy(c.b[offset:], row)

	c.b[offset] = byte(len(family))
	offset++
	offset += copy(c.b[offset:], family)

	offset += copy(c.b[offset:], qualifier)

	binary.BigEndian.PutUint64(c.b[offset:offset+8], timestamp)
	offset += 8

	c.b[offset] = byte(celltype)
	offset++

	copy(c.b[offset:], value)

	return c, nil
}

func (c CellV2) Equal(d CellV2) bool {
	return c.qualifierLen == d.qualifierLen &&
		c.rowLen == d.rowLen &&
		c.familyLen == d.familyLen &&
		bytes.Equal(c.b, d.b)
}

func (c CellV2) String() string {
	return fmt.Sprintf(
		"CellV2{Row: %q Family: %q Qualifier: %q Timestamp: %d Value: %q CellType: %s}",
		c.Row(), c.Family(), c.Qualifier(), c.Timestamp(), c.Value(), c.CellType())
}

const rowOffset uint32 = 14

// Row returns the row key
func (c CellV2) Row() []byte {
	return slices.Clip(c.b[rowOffset : rowOffset+uint32(c.rowLen)])
}

// Family returns the column family
func (c CellV2) Family() []byte {
	start := rowOffset + uint32(c.rowLen) + 1
	return slices.Clip(c.b[start : start+uint32(c.familyLen)])
}

// Qualifier returns the qualifier/column key
func (c CellV2) Qualifier() []byte {
	start := rowOffset + uint32(c.rowLen) + 1 + uint32(c.familyLen)
	return slices.Clip(c.b[start : start+c.qualifierLen])
}

// Timestamps returns the timestamp
func (c CellV2) Timestamp() uint64 {
	start := rowOffset + uint32(c.rowLen) + 1 + uint32(c.familyLen) + c.qualifierLen
	return binary.BigEndian.Uint64(c.b[start : start+8])
}

// CellType returns this cell's type.
func (c CellV2) CellType() pb.CellType {
	start := rowOffset + uint32(c.rowLen) + 1 + uint32(c.familyLen) + c.qualifierLen + 8
	return pb.CellType(c.b[start])
}

// Value returns the value
func (c CellV2) Value() []byte {
	start := rowOffset + uint32(c.rowLen) + 1 + uint32(c.familyLen) + c.qualifierLen + 8 + 1
	return slices.Clip(c.b[start:])
}

func deserializeCellBlocksV2(b []byte, cellsLen uint32) ([]CellV2, uint32, error) {
	cells := make([]CellV2, cellsLen)
	var readLen uint32
	for i := 0; i < int(cellsLen); i++ {
		c, l, err := cellV2FromCellBlock(b[readLen:])
		if err != nil {
			return nil, readLen, err
		}
		cells[i] = c
		readLen += l
	}
	return cells, readLen, nil
}

// ToPBCell returns this cell as a protobuf struct for a Cell.
func (c CellV2) ToPBCell() *pb.Cell {
	ts := c.Timestamp()
	return &pb.Cell{
		Row:       c.Row(),
		Family:    c.Family(),
		Qualifier: c.Qualifier(),
		Timestamp: &ts,
		Value:     c.Value(),
		CellType:  c.CellType().Enum(),
	}
}
