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

	family []byte

	closestBefore bool
}

// NewGetStr creates a new Get request for the given table/key.
func NewGetStr(table, key string) *Get {
	return &Get{
		base: base{
			table: []byte(table),
			key:   []byte(key),
		},
		// TODO
	}
}

// NewGetBefore creates a new Get request for the row right before the given
// key in the given table and family.
func NewGetBefore(table, key, family []byte) *Get {
	return &Get{
		base: base{
			table: table,
			key:   key,
		},
		family:        family,
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
			Row: g.key,
		},
	}
	if g.family != nil {
		get.Get.Column = []*pb.Column{
			&pb.Column{
				Family: g.family,
				//Qualifier: [][]byte{[]byte("theCol")},
			},
		}
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
