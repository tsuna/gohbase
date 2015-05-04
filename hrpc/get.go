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

	family []byte

	closestBefore bool
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

func (g *Get) Name() string {
	return "Get"
}

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

func (g *Get) NewResponse() proto.Message {
	return &pb.GetResponse{}
}
