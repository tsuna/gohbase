// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

// Get represents a Get HBase call.
type Get struct {
	base

	families map[string][]string //Maps a column family to a list of qualifiers

	closestBefore bool
}

// NewGetStr creates a new Get request for the given table/key.
func NewGetStr(table, key string, families map[string][]string) *Get {
	return &Get{
		base: base{
			table: []byte(table),
			key:   []byte(key),
		},
		families: families,
	}
}

// NewGetBefore creates a new Get request for the row right before the given
// key in the given table and family.
func NewGetBefore(table, key []byte, families map[string][]string) *Get {
	return &Get{
		base: base{
			table: table,
			key:   key,
		},
		families:      families,
		closestBefore: true,
	}
}

// Name returns the name of this RPC call.
func (g *Get) Name() string {
	return "Get"
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
