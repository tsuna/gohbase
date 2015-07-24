// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

// CreateTable represents a CreateTable HBase call
type CreateTable struct {
	base

	columns []string
}

// NewCreateTable creates a new CreateTable request that will create the given
// table in HBase. For use by the admin client.
func NewCreateTable(ctx context.Context, table []byte, columns []string) *CreateTable {
	ct := &CreateTable{
		base: base{
			table: table,
			ctx:   ctx,
		},
		columns: columns,
	}
	return ct
}

// GetName returns the name of this RPC call.
func (ct *CreateTable) GetName() string {
	return "CreateTable"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (ct *CreateTable) Serialize() ([]byte, error) {
	pbcols := make([]*pb.ColumnFamilySchema, len(ct.columns))
	for i, col := range ct.columns {
		pbcols[i] = &pb.ColumnFamilySchema{
			Name: []byte(col),
		}
	}
	ctable := &pb.CreateTableRequest{
		TableSchema: &pb.TableSchema{
			TableName: &pb.TableName{
				Namespace: []byte("default"),
				Qualifier: ct.table,
			},
			ColumnFamilies: pbcols,
		},
	}
	return proto.Marshal(ctable)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ct *CreateTable) NewResponse() proto.Message {
	return &pb.CreateTableResponse{}
}

// SetFilter always returns an error when used on Mutate objects. Do not use.
// Exists solely so Mutate can implement the Call interface.
func (ct *CreateTable) SetFilter(ft filter.Filter) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set filter on mutate operation.")
}

// SetFamilies always returns an error when used on Mutate objects. Do not use.
// Exists solely so Mutate can implement the Call interface.
func (ct *CreateTable) SetFamilies(fam map[string][]string) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set families on mutate operation.")
}
