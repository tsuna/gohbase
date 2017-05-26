// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
)

// Get represents a Get HBase call.
type Get struct {
	base

	families map[string][]string //Maps a column family to a list of qualifiers

	// Return the row for the given key or, if this key doesn't exist,
	// whichever key happens to be right before.
	closestBefore bool

	// Don't return any KeyValue, just say whether the row key exists in the
	// table or not.
	existsOnly bool

	fromTimestamp uint64
	toTimestamp   uint64

	maxVersions uint32

	filters filter.Filter
}

// baseGet returns a Get struct with default values set.
func baseGet(ctx context.Context, table []byte, key []byte,
	options ...func(Call) error) (*Get, error) {
	g := &Get{
		base: base{
			key:   key,
			table: table,
			ctx:   ctx,
		},
		fromTimestamp: MinTimestamp,
		toTimestamp:   MaxTimestamp,
		maxVersions:   DefaultMaxVersions,
	}
	err := applyOptions(g, options...)
	if err != nil {
		return nil, err
	}
	return g, nil
}

// NewGet creates a new Get request for the given table and row key.
func NewGet(ctx context.Context, table, key []byte,
	options ...func(Call) error) (*Get, error) {
	return baseGet(ctx, table, key, options...)
}

// NewGetStr creates a new Get request for the given table and row key.
func NewGetStr(ctx context.Context, table, key string,
	options ...func(Call) error) (*Get, error) {
	return NewGet(ctx, []byte(table), []byte(key), options...)
}

// NewGetBefore creates a new Get request for the row with a key equal to or
// immediately less than the given key, in the given table.
func NewGetBefore(ctx context.Context, table, key []byte,
	options ...func(Call) error) (*Get, error) {
	g, err := baseGet(ctx, table, key, options...)
	if err != nil {
		return nil, err
	}
	g.closestBefore = true
	return g, nil
}

// Name returns the name of this RPC call.
func (g *Get) Name() string {
	return "Get"
}

// Filter returns the filter of this Get request.
func (g *Get) Filter() filter.Filter {
	return g.filters
}

// Families returns the families to retrieve with this Get request.
func (g *Get) Families() map[string][]string {
	return g.families
}

// SetFilter sets filter to use for this Get request.
func (g *Get) SetFilter(f filter.Filter) error {
	g.filters = f
	// TODO: Validation?
	return nil
}

// SetFamilies sets families to retrieve with this Get request.
func (g *Get) SetFamilies(f map[string][]string) error {
	g.families = f
	// TODO: Validation?
	return nil
}

// ExistsOnly makes this Get request not return any KeyValue, merely whether
// or not the given row key exists in the table.
func (g *Get) ExistsOnly() {
	g.existsOnly = true
}

// ToProto converts this RPC into a protobuf message.
func (g *Get) ToProto() (proto.Message, error) {
	get := &pb.GetRequest{
		Region: g.regionSpecifier(),
		Get: &pb.Get{
			Row:       g.key,
			Column:    familiesToColumn(g.families),
			TimeRange: &pb.TimeRange{},
		},
	}
	if g.maxVersions != DefaultMaxVersions {
		get.Get.MaxVersions = &g.maxVersions
	}
	if g.fromTimestamp != MinTimestamp {
		get.Get.TimeRange.From = &g.fromTimestamp
	}
	if g.toTimestamp != MaxTimestamp {
		get.Get.TimeRange.To = &g.toTimestamp
	}
	if g.closestBefore {
		get.Get.ClosestRowBefore = proto.Bool(true)
	}
	if g.existsOnly {
		get.Get.ExistenceOnly = proto.Bool(true)
	}
	if g.filters != nil {
		pbFilter, err := g.filters.ConstructPBFilter()
		if err != nil {
			return nil, err
		}
		get.Get.Filter = pbFilter
	}
	return get, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (g *Get) NewResponse() proto.Message {
	return &pb.GetResponse{}
}

// DeserializeCellBlocks deserializes get result from cell blocks
func (g *Get) DeserializeCellBlocks(m proto.Message, r io.Reader, cellsLen uint32) error {
	getResp := m.(*pb.GetResponse)
	if getResp.Result == nil {
		// TODO: is this possible?
		return nil
	}
	cells, err := deserializeCellBlocks(r, cellsLen)
	if err != nil {
		return err
	}
	getResp.Result.Cell = append(getResp.Result.Cell, cells...)
	return nil
}

// familiesToColumn takes a map from strings to lists of strings, and converts
// them into protobuf Columns
func familiesToColumn(families map[string][]string) []*pb.Column {
	cols := make([]*pb.Column, len(families))
	counter := 0
	for family, qualifiers := range families {
		bytequals := make([][]byte, len(qualifiers))
		for i, qual := range qualifiers {
			bytequals[i] = []byte(qual)
		}
		cols[counter] = &pb.Column{
			Family:    []byte(family),
			Qualifier: bytequals,
		}
		counter++
	}
	return cols
}
