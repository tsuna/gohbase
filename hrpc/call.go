// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
)

// RegionInfo represents HBase region.
type RegionInfo interface {
	IsUnavailable() bool
	AvailabilityChan() <-chan struct{}
	MarkUnavailable() bool
	MarkAvailable()
	MarkDead()
	Context() context.Context
	String() string
	ID() uint64
	Name() []byte
	StartKey() []byte
	StopKey() []byte
	Namespace() []byte
	Table() []byte
	SetClient(RegionClient)
	Client() RegionClient
}

// RegionClient represents HBase region client.
type RegionClient interface {
	Close()
	Host() string
	Port() uint16
	QueueRPC(Call)
	String() string
}

// Call represents an HBase RPC call.
type Call interface {
	Table() []byte
	Name() string
	Key() []byte
	Region() RegionInfo
	SetRegion(region RegionInfo)
	ToProto() (proto.Message, error)
	// Returns a newly created (default-state) protobuf in which to store the
	// response of this call.
	NewResponse() proto.Message
	ResultChan() chan RPCResult
	Context() context.Context
	SetFamilies(fam map[string][]string) error
	SetFilter(ft filter.Filter) error
}

// RPCResult is struct that will contain both the resulting message from an RPC
// call, and any errors that may have occurred related to making the RPC call.
type RPCResult struct {
	Msg   proto.Message
	Error error
}

type base struct {
	table []byte

	key []byte

	region RegionInfo

	// Protects access to resultch.
	resultchLock sync.Mutex

	resultch chan RPCResult

	ctx context.Context
}

func (b *base) Context() context.Context {
	return b.ctx
}

func (b *base) Region() RegionInfo {
	return b.region
}

func (b *base) SetRegion(region RegionInfo) {
	b.region = region
}

func (b *base) regionSpecifier() *pb.RegionSpecifier {
	regionType := pb.RegionSpecifier_REGION_NAME
	return &pb.RegionSpecifier{
		Type:  &regionType,
		Value: []byte(b.region.Name()),
	}
}

func applyOptions(call Call, options ...func(Call) error) error {
	for _, option := range options {
		err := option(call)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *base) Table() []byte {
	return b.table
}

func (b *base) Key() []byte {
	return b.key
}

func (b *base) ResultChan() chan RPCResult {
	b.resultchLock.Lock()
	if b.resultch == nil {
		// Buffered channels, so that if a writer thread sends a message (or
		// reports an error) after the deadline it doesn't block due to the
		// requesting thread having moved on.
		b.resultch = make(chan RPCResult, 1)
	}
	b.resultchLock.Unlock()
	return b.resultch
}

// Families is used as a parameter for request creation. Adds families constraint to a request.
func Families(fam map[string][]string) func(Call) error {
	return func(g Call) error {
		return g.SetFamilies(fam)
	}
}

// Filters is used as a parameter for request creation. Adds filters constraint to a request.
func Filters(fl filter.Filter) func(Call) error {
	return func(g Call) error {
		return g.SetFilter(fl)
	}
}

// TimeRange is used as a parameter for request creation. Adds TimeRange constraint to a request.
// It will get values in range [from, to[ ('to' is exclusive).
func TimeRange(from, to time.Time) func(Call) error {
	return TimeRangeUint64(uint64(from.UnixNano()/1e6), uint64(to.UnixNano()/1e6))
}

// TimeRangeUint64 is used as a parameter for request creation.
// Adds TimeRange constraint to a request.
// from and to should be in milliseconds
// // It will get values in range [from, to[ ('to' is exclusive).
func TimeRangeUint64(from, to uint64) func(Call) error {
	return func(g Call) error {
		if from >= to {
			// or equal is becuase 'to' is exclusive
			return fmt.Errorf("'from' timestamp (%dms) is greater"+
				" or equal to 'to' timestamp (%dms)",
				from, to)
		}
		switch c := g.(type) {
		default:
			return errors.New("'TimeRange' option can only be used with Get or Scan queries")
		case *Get:
			c.fromTimestamp = from
			c.toTimestamp = to
		case *Scan:
			c.fromTimestamp = from
			c.toTimestamp = to
		}
		return nil
	}
}

// MaxVersions is used as a parameter for request creation.
// Adds MaxVersions constraint to a request.
func MaxVersions(versions uint32) func(Call) error {
	return func(g Call) error {
		switch c := g.(type) {
		default:
			return errors.New("'MaxVersions' option can only be used with Get or Scan queries")
		case *Get:
			if versions > math.MaxInt32 {
				return errors.New("'MaxVersions' exceeds supported number of versions")
			}
			c.maxVersions = versions
		case *Scan:
			if versions > math.MaxInt32 {
				return errors.New("'MaxVersions' exceeds supported number of versions")
			}
			c.maxVersions = versions
		}
		return nil
	}
}

// MaxResultsPerColumnFamily is an option for Get or Scan requests that sets the maximum
// number of cells returned per column family in a row
func MaxResultsPerColumnFamily(maxresults uint32) func(Call) error {
	opt := "'MaxResultsperColumnFamily' "
	return func(g Call) error {
		switch c := g.(type) {
		default:
			return errors.New(opt + "option can only be used with Get or Scan queries")
		case *Get:
			if maxresults > math.MaxInt32 {
				return errors.New(opt + "exceeds supported number of value results")
			}
			c.storeLimit = maxresults
		case *Scan:
			if maxresults > math.MaxInt32 {
				return errors.New(opt + "exceeds supported number of value results")
			}
			c.storeLimit = maxresults
		}
		return nil
	}
}

// ResultOffset is a option for Scan or Get requests that sets the offset for cells
// within a column family
func ResultOffset(offset uint32) func(Call) error {
	return func(g Call) error {
		switch c := g.(type) {
		default:
			return errors.New("'ResultOffset' option can only be used with Get or Scan queries")
		case *Get:
			if offset > math.MaxInt32 {
				return errors.New("'ResultOffset' exceeds supported offset value")
			}
			c.storeOffset = offset
		case *Scan:
			if offset > math.MaxInt32 {
				return errors.New("'ResultOffset' exceeds supported offset value")
			}
			c.storeOffset = offset
		}
		return nil
	}
}

// Cell is the smallest level of granularity in returned results.
// Represents a single cell in HBase (a row will have one cell for every qualifier).
type Cell pb.Cell

// cellFromCellBlock deserializes a cell from a reader
func cellFromCellBlock(b []byte) (*pb.Cell, uint32, error) {
	if len(b) < 4 {
		return nil, 0, fmt.Errorf(
			"buffer is too small: expected %d, got %d", 4, len(b))
	}

	kvLen := binary.BigEndian.Uint32(b[0:4])
	if len(b) < int(kvLen)+4 {
		return nil, 0, fmt.Errorf(
			"buffer is too small: expected %d, got %d", int(kvLen)+4, len(b))
	}

	rowKeyLen := binary.BigEndian.Uint32(b[4:8])
	valueLen := binary.BigEndian.Uint32(b[8:12])
	keyLen := binary.BigEndian.Uint16(b[12:14])
	b = b[14:]

	key := b[:keyLen]
	b = b[keyLen:]

	familyLen := uint8(b[0])
	b = b[1:]

	family := b[:familyLen]
	b = b[familyLen:]

	qualifierLen := rowKeyLen - uint32(keyLen) - uint32(familyLen) - 2 - 1 - 8 - 1
	if 4 /*rowKeyLen*/ +4 /*valueLen*/ +2 /*keyLen*/ +
		uint32(keyLen)+1 /*familyLen*/ +uint32(familyLen)+qualifierLen+
		8 /*timestamp*/ +1 /*cellType*/ +valueLen != kvLen {
		return nil, 0, fmt.Errorf("HBase has lied about KeyValue length: expected %d, got %d",
			kvLen, 4+4+2+uint32(keyLen)+1+uint32(familyLen)+qualifierLen+8+1+valueLen)
	}
	qualifier := b[:qualifierLen]
	b = b[qualifierLen:]

	timestamp := binary.BigEndian.Uint64(b[:8])
	b = b[8:]

	cellType := uint8(b[0])
	b = b[1:]

	value := b[:valueLen]

	return &pb.Cell{
		Row:       key,
		Family:    family,
		Qualifier: qualifier,
		Timestamp: &timestamp,
		Value:     value,
		CellType:  pb.CellType(cellType).Enum(),
	}, kvLen + 4, nil
}

func deserializeCellBlocks(b []byte, cellsLen uint32) ([]*pb.Cell, uint32, error) {
	cells := make([]*pb.Cell, cellsLen)
	var readLen uint32
	for i := 0; i < int(cellsLen); i++ {
		c, l, err := cellFromCellBlock(b[readLen:])
		if err != nil {
			return nil, readLen, err
		}
		cells[i] = c
		readLen += l
	}
	return cells, readLen, nil
}

// Result holds a slice of Cells as well as miscellaneous information about the response.
type Result struct {
	Cells   []*Cell
	Stale   bool
	Partial bool
	// Exists is only set if existance_only was set in the request query.
	Exists *bool
}

func extractBool(v *bool) bool {
	return v != nil && *v
}

// ToLocalResult takes a protobuf Result type and converts it to our own
// Result type in constant time.
func ToLocalResult(pbr *pb.Result) *Result {
	if pbr == nil {
		return &Result{}
	}
	return &Result{
		// Should all be O(1) operations.
		Cells:   toLocalCells(pbr),
		Stale:   extractBool(pbr.Stale),
		Partial: extractBool(pbr.Partial),
		Exists:  pbr.Exists,
	}
}

func toLocalCells(pbr *pb.Result) []*Cell {
	return *(*[]*Cell)(unsafe.Pointer(pbr))
}

// We can now define any helper functions on Result that we want.
