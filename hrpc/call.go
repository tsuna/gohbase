// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/regioninfo"
	"golang.org/x/net/context"
	"reflect"
	"unsafe"
)

// Call represents an HBase RPC call.
type Call interface {
	Table() []byte

	Key() []byte

	GetRegion() *regioninfo.Info
	SetRegion(region *regioninfo.Info)
	GetName() string
	Serialize() ([]byte, error)
	// Returns a newly created (default-state) protobuf in which to store the
	// response of this call.
	NewResponse() proto.Message

	GetResultChan() chan RPCResult

	GetContext() context.Context

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

	region *regioninfo.Info

	resultch chan RPCResult

	ctx context.Context
}

func (b *base) GetContext() context.Context {
	return b.ctx
}

func (b *base) GetRegion() *regioninfo.Info {
	return b.region
}

func (b *base) SetRegion(region *regioninfo.Info) {
	b.region = region
}

func (b *base) regionSpecifier() *pb.RegionSpecifier {
	regionType := pb.RegionSpecifier_REGION_NAME
	return &pb.RegionSpecifier{
		Type:  &regionType,
		Value: []byte(b.region.RegionName),
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

func (b *base) GetResultChan() chan RPCResult {
	if b.resultch == nil {
		// Buffered channels, so that if a writer thread sends a message (or
		// reports an error) after the deadline it doesn't block due to the
		// requesting thread having moved on.
		b.resultch = make(chan RPCResult, 1)
	}
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

type Result pb.Result
type Cell pb.Cell
type CellType pb.CellType

func (r *Result) GetCells() []*Cell {
	// We have two options here, both of which are pretty terrible.
	// Problem is we've aliased pb.Result but the field Cell is still the type []*pb.Cell
	// Because we've also aliased pb.Cell -> Cell we'd like to be able to return []*Cell for
	// GetCells().

	// Option 1: Use the unsafe package and do a simple in place pointer conversion.
	//
	//	Pros:	Very fast. We're only adjusting the type of the header pointer.
	//
	//	Cons: 	Breaks type system. "Packages that import unsafe may be non-portable and
	//		are not protected by the Go 1 compatibility guidelines."

	header := *(*reflect.SliceHeader)(unsafe.Pointer(&r.Cell))
	return *(*[]*Cell)(unsafe.Pointer(&header))

	// Option 2: Iterate over the list and do a conversion on every pointer.
	//
	//	Pros:	Type safe. Idiomatic.
	//
	//	Cons:	Relatively slow. While we're only copying pointers we're still doing
	//		unneccesary work just to hide the pb layer. Also the number of cells can
	//		be very large and we need to be wary of any extra O(n) operations we're
	//		doing on them.

	cellSlice := make([]*Cell, len(r.Cell))
	for i := range r.Cell {
		cellSlice[i] = (*Cell)(r.Cell[i])
	}
	return cellSlice
}

func (c *Cell) GetCellType() CellType {
	return CellType(*c.CellType)
}
