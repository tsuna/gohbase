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

/*
    See: http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
    Allows usage like the following -

	rsp, err := hrpc.NewGet(table, key)
	rsp, err := hrpc.NewGet(table, key, hrpc.Context(context))
	rsp, err := hrpc.NewGet(table, key, hrpc.Families(fam))
	rsp, err := hrpc.NewGet(table, key, hrpc.Filters(filter))
	rsp, err := hrpc.NewGet(table, key, hrpc.Context(context), hrpc.Families(fam), hrpc.Filters(filter))

	Context is defined hrpc/call.go:100
	Families is defined hrpc/call.go:

*/
func NewGet(table, key []byte, options ...func(Call) error) (*Get, error) {
	g := &Get{
		base: base{
			table: table,
			key:   key,
			ctx:   context.Background(), // Is this a proper default context?
		},
	}
	for _, option := range options {
		err := option(g)
		if err != nil {
			return nil, err
		}
	}
	return g, nil
}

// NewGetStr creates a new Get request for the given table/key.
func NewGetStr(ctx context.Context, table, key string) *Get {
	return &Get{
		base: base{
			table: []byte(table),
			key:   []byte(key),
			ctx:   ctx,
		},
	}
}

// NewGetBefore creates a new Get request for the row right before the given
// key in the given table and family.
func NewGetBefore(ctx context.Context, table, key []byte, families map[string][]string) *Get {
	return &Get{
		base: base{
			table: table,
			key:   key,
			ctx:   ctx,
		},
		families:      families,
		closestBefore: true,
	}
}

// Name returns the name of this RPC call.
func (g *Get) Name() string {
	return "Get"
}

// Filter returns current set filter
func (g *Get) GetFilter() filter.Filter {
	return g.filters
}

// Families returns current set family
func (g *Get) Families() map[string][]string {
	return g.families
}

// SetFilter sets filter to use and returns the object
func (g *Get) SetFilter(f filter.Filter) error {
	g.filters = f
	return nil
}

// SetFamilies sets families to use and returns the object
func (g *Get) SetFamilies(f map[string][]string) error {
	g.families = f
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
