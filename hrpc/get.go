// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

// Get represents a Get HBase call.
type Get struct {
	base

	families map[string][]string //Maps a column family to a list of qualifiers

	closestBefore bool

	filters filter.Filter
}

// NewGet is called to construct a Get* object which is then passed as the sole parameter for a
// client.Get(). Uses functional options for arguments, for more information see -
// http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
// Allows usage like the following -
//	get, err := hrpc.NewGet(ctx, table, key)
//	get, err := hrpc.NewGet(ctx, table, key, hrpc.Families(fam))
//	get, err := hrpc.NewGet(ctx, table, key, hrpc.Filters(filter))
//	get, err := hrpc.NewGet(ctx, table, key, hrpc.Families(fam), hrpc.Filters(filter))
func NewGet(ctx context.Context, table, key []byte, options ...func(Call) error) (*Get, error) {
	g := &Get{
		base: base{
			table: table,
			key:   key,
			ctx:   ctx,
		},
	}
	err := applyOptions(g, options...)
	if err != nil {
		return nil, err
	}
	return g, nil
}

// NewGetStr wraps NewGet to allow string table names and keys.
func NewGetStr(ctx context.Context, table, key string, options ...func(Call) error) (*Get, error) {
	return NewGet(ctx, []byte(table), []byte(key), options...)
}

// NewGetBefore creates a new Get request for the row right before the given
// key in the given table and family. Accepts functional options.
func NewGetBefore(ctx context.Context, table, key []byte, options ...func(Call) error) (*Get, error) {
	g := &Get{
		base: base{
			table: table,
			key:   key,
			ctx:   ctx,
		},
		closestBefore: true,
	}
	err := applyOptions(g, options...)
	if err != nil {
		return nil, err
	}
	return g, nil
}

// GetName returns the name of this RPC call.
func (g *Get) GetName() string {
	return "Get"
}

// GetFilter returns current set filter
func (g *Get) GetFilter() filter.Filter {
	return g.filters
}

// GetFamilies returns current set family
func (g *Get) GetFamilies() map[string][]string {
	return g.families
}

// SetFilter sets filter to use and returns the object
func (g *Get) SetFilter(f filter.Filter) error {
	g.filters = f
	// TODO: Validation?
	return nil
}

// SetFamilies sets families to use and returns the object
func (g *Get) SetFamilies(f map[string][]string) error {
	g.families = f
	// TODO: Validation?
	return nil
}

// Serialize serializes this RPC into a buffer.
func (g *Get) Serialize() ([]byte, error) {
	get := &pb.GetRequest{
		Region: g.regionSpecifier(),
		Get: &pb.Get{
			Row:    g.key,
			Column: familiesToColumn(g.families),
		},
	}
	if g.closestBefore {
		get.Get.ClosestRowBefore = proto.Bool(true)
	}
	if g.filters != nil {
		pbFilter, err := g.filters.ConstructPBFilter()
		if err != nil {
			return nil, err
		}
		get.Get.Filter = pbFilter
	}
	return proto.Marshal(get)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (g *Get) NewResponse() proto.Message {
	return &pb.GetResponse{}
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
