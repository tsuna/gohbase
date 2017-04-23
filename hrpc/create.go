// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

// CreateTable represents a CreateTable HBase call
type CreateTable struct {
	tableOp

	families map[string]map[string]string
}

var defaultAttributes = map[string]string{
	"BLOOMFILTER":         "ROW",
	"VERSIONS":            "3",
	"IN_MEMORY":           "false",
	"KEEP_DELETED_CELLS":  "false",
	"DATA_BLOCK_ENCODING": "FAST_DIFF",
	"TTL":               "2147483647",
	"COMPRESSION":       "NONE",
	"MIN_VERSIONS":      "0",
	"BLOCKCACHE":        "true",
	"BLOCKSIZE":         "65536",
	"REPLICATION_SCOPE": "0",
}

// NewCreateTable creates a new CreateTable request that will create the given
// table in HBase. 'families' is a map of column family name to its attributes.
// For use by the admin client.
func NewCreateTable(ctx context.Context, table []byte,
	families map[string]map[string]string) *CreateTable {
	ct := &CreateTable{
		tableOp: tableOp{base{
			table: table,
			ctx:   ctx,
		}},
		families: make(map[string]map[string]string, len(families)),
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
func (ct *CreateTable) Name() string {
	return "CreateTable"
}

// ToProto converts the RPC into a protobuf message
func (ct *CreateTable) ToProto() (proto.Message, error) {
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
	return &pb.CreateTableRequest{
		TableSchema: &pb.TableSchema{
			TableName: &pb.TableName{
				// TODO: handle namespaces
				Namespace: []byte("default"),
				Qualifier: ct.table,
			},
			ColumnFamilies: pbFamilies,
		},
	}, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ct *CreateTable) NewResponse() proto.Message {
	return &pb.CreateTableResponse{}
}
