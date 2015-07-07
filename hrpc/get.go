// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

// Get represents a Get HBase call.
type Get struct {
	base

	families map[string][]string //Maps a column family to a list of qualifiers

	closestBefore bool

	filters Filter
}

// NewGetStr creates a new Get request for the given table/key.
func NewGetStr(ctx context.Context, table, key string, families map[string][]string, filters Filter) *Get {
	return &Get{
		base: base{
			table: []byte(table),
			key:   []byte(key),
			ctx:   ctx,
		},
		families: families,
		filters:  filters,
	}
}

// NewGetBefore creates a new Get request for the row right before the given
// key in the given table and family.
func NewGetBefore(ctx context.Context, table, key []byte, families map[string][]string, filters Filter) *Get {
	return &Get{
		base: base{
			table: table,
			key:   key,
			ctx:   ctx,
		},
		families:      families,
		closestBefore: true,
		filters:       filters,
	}
}

// Name returns the name of this RPC call.
func (g *Get) Name() string {
	return "Get"
}

// Serialize serializes this RPC into a buffer.
func (g *Get) Serialize() ([]byte, error) {
	var pbGet *pb.Get
	if g.filters == nil {
		pbGet = &pb.Get{
			Row:    g.key,
			Column: familiesToColumn(g.families),
		}
	} else {
		pbFilters, err := g.filters.ConstructPBFilter()
		if err != nil {
			return nil, err
		}
		pbGet = &pb.Get{
			Row:    g.key,
			Column: familiesToColumn(g.families),
			Filter: pbFilters,
		}
	}

	get := &pb.GetRequest{
		Region: g.regionSpecifier(),
		Get:    pbGet,
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
