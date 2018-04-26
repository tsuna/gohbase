// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

// AddColumn represents a AddColumn HBase call
type AddColumn struct {
	base

	families  map[string]map[string]string
}

// NewAddColumn creates a new AddColumn request that will create the given
// table in HBase. 'families' is a map of column family name to its attributes.
// For use by the admin client.
func NewAddColumn(ctx context.Context, table []byte,
	families map[string]map[string]string,
	options ...func(*AddColumn)) *AddColumn {
	ct := &AddColumn{
		base: base{
			table: table,
			ctx:   ctx,
		},
		families: make(map[string]map[string]string, len(families)),
	}
	for _, option := range options {
		option(ct)
	}
	for family, attrs := range families {
		ct.families[family] = make(map[string]string, len(defaultAttributes))
		for k, dv := range defaultAttributes {
			if v, ok := attrs[k]; ok {
				ct.families[family][k] = v
			} else {
				ct.families[family][k] = dv
			}
		}
	}
	return ct
}

// Name returns the name of this RPC call.
func (ct *AddColumn) Name() string {
	return "AddColumn"
}

// ToProto converts the RPC into a protobuf message
func (ct *AddColumn) ToProto() proto.Message {
	pbFamilies := make([]*pb.ColumnFamilySchema, 0, len(ct.families))
	for family, attrs := range ct.families {
		f := &pb.ColumnFamilySchema{
			Name:       []byte(family),
			Attributes: make([]*pb.BytesBytesPair, 0, len(attrs)),
		}
		for k, v := range attrs {
			f.Attributes = append(f.Attributes, &pb.BytesBytesPair{
				First:  []byte(k),
				Second: []byte(v),
			})
		}
		pbFamilies = append(pbFamilies, f)
	}
	namespace := []byte("default")
	table := ct.table
	i := bytes.Index(table, []byte(":"))
	if i > -1 {
		namespace = table[:i]
		table = table[i+1:]
	}
	return &pb.AddColumnRequest{
		TableName: &pb.TableName{
				Namespace: namespace,
				Qualifier: table,
		},
		ColumnFamilies: pbFamilies[0],
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ct *AddColumn) NewResponse() proto.Message {
	return &pb.AddColumnResponse{}
}
