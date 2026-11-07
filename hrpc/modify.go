// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// ModifyTable represents a ModifyTable HBase call.
type ModifyTable struct {
	base
	schema *pb.TableSchema
}

// NewModifyTable creates a new ModifyTable request that will modify the schema
// of an existing table. The caller is responsible for providing the full
// desired TableSchema (typically obtained via GetTableSchema, then modified).
func NewModifyTable(ctx context.Context, table []byte,
	schema *pb.TableSchema) (*ModifyTable, error) {
	if schema == nil {
		return nil, errors.New("ModifyTable requires a non-nil TableSchema")
	}
	return &ModifyTable{
		base: base{
			table:    table,
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		schema: schema,
	}, nil
}

// Name returns the name of this RPC call.
func (mt *ModifyTable) Name() string {
	return "ModifyTable"
}

// Description returns the description of this RPC call.
func (mt *ModifyTable) Description() string {
	return mt.Name()
}

// ToProto converts the RPC into a protobuf message.
func (mt *ModifyTable) ToProto() proto.Message {
	ns := mt.schema.GetTableName().GetNamespace()
	if ns == nil {
		ns = []byte("default")
	}
	return &pb.ModifyTableRequest{
		TableName: &pb.TableName{
			Namespace: ns,
			Qualifier: mt.table,
		},
		TableSchema: mt.schema,
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (mt *ModifyTable) NewResponse() proto.Message {
	return &pb.ModifyTableResponse{}
}
