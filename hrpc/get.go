// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

type Get struct {
	base

	// TODO: More...
}

func NewGetStr(table, key string) *Get {
	return &Get{
		base: base{
			table: []byte(table),
			key:   []byte(key),
		},
		// TODO
	}
}

func (g *Get) Name() string {
	return "Get"
}

func (g *Get) Serialize() ([]byte, error) {
	get := &pb.GetRequest{
		Region: g.regionSpecifier(),
		Get: &pb.Get{
			Row: []byte("theKey"),
			Column: []*pb.Column{
				&pb.Column{
					Family:    []byte("e"),
					Qualifier: [][]byte{[]byte("theCol")},
				},
			},
		},
	}
	return proto.Marshal(get)
}
