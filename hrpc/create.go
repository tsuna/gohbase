// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// CreateTable represents a CreateTable HBase call
type CreateTable struct {
	base

	families      map[string]map[string]string
	splitKeys     [][]byte
	attributes    map[string]string
	configuration map[string]string
}

var defaultAttributes = map[string]string{
	"BLOOMFILTER":         "ROW",
	"VERSIONS":            "3",
	"IN_MEMORY":           "false",
	"KEEP_DELETED_CELLS":  "false",
	"DATA_BLOCK_ENCODING": "FAST_DIFF",
	"TTL":                 "2147483647",
	"COMPRESSION":         "NONE",
	"MIN_VERSIONS":        "0",
	"BLOCKCACHE":          "true",
	"BLOCKSIZE":           "65536",
	"REPLICATION_SCOPE":   "0",
}

// NewCreateTable creates a new CreateTable request that will create the given
// table in HBase. 'families' is a map of column family name to its attributes.
// For use by the admin client.
func NewCreateTable(ctx context.Context, table []byte,
	families map[string]map[string]string,
	options ...func(*CreateTable)) *CreateTable {
	ct := &CreateTable{
		base: base{
			table:    table,
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		families:      make(map[string]map[string]string, len(families)),
		attributes:    make(map[string]string),
		configuration: make(map[string]string),
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

// SplitKeys will return an option that will set the split keys for the created table
func SplitKeys(sk [][]byte) func(*CreateTable) {
	return func(ct *CreateTable) {
		ct.splitKeys = sk
	}
}

// Attributes will add create table attributes params
func Attributes(key, value string) func(table *CreateTable) {
	return func(ct *CreateTable) {
		ct.attributes[key] = value
	}
}

// Configuration will add create table conf
func Configuration(name, value string) func(table *CreateTable) {
	return func(ct *CreateTable) {
		ct.configuration[name] = value
	}
}

// Name returns the name of this RPC call.
func (ct *CreateTable) Name() string {
	return "CreateTable"
}

// Description returns the description of this RPC call.
func (ct *CreateTable) Description() string {
	return ct.Name()
}

// ToProto converts the RPC into a protobuf message
func (ct *CreateTable) ToProto() proto.Message {
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

	// Attributes
	pbAttributes := make([]*pb.BytesBytesPair, 0, len(ct.attributes))
	for k, v := range ct.attributes {
		pbAttributes = append(pbAttributes, &pb.BytesBytesPair{
			First:  []byte(k),
			Second: []byte(v),
		})
	}

	// Configuration
	pbConfiguration := make([]*pb.NameStringPair, 0, len(ct.configuration))
	for k, v := range ct.configuration {
		pbConfiguration = append(pbConfiguration, &pb.NameStringPair{
			Name:  proto.String(k),
			Value: proto.String(v),
		})
	}

	// TableName
	namespace, table := ct.parseTableName()
	return &pb.CreateTableRequest{
		TableSchema: &pb.TableSchema{
			TableName: &pb.TableName{
				Namespace: namespace,
				Qualifier: table,
			},
			ColumnFamilies: pbFamilies,
			Attributes:     pbAttributes,
			Configuration:  pbConfiguration,
		},
		SplitKeys: ct.splitKeys,
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ct *CreateTable) NewResponse() proto.Message {
	return &pb.CreateTableResponse{}
}
