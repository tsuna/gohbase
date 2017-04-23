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

// DisableTable represents a DisableTable HBase call
type DisableTable struct {
	tableOp
}

// NewDisableTable creates a new DisableTable request that will disable the
// given table in HBase. For use by the admin client.
func NewDisableTable(ctx context.Context, table []byte) *DisableTable {
	return &DisableTable{
		tableOp{base{
			table: table,
			ctx:   ctx,
		}},
	}
}

// Name returns the name of this RPC call.
func (dt *DisableTable) Name() string {
	return "DisableTable"
}

// ToProto converts the RPC into a protobuf message
func (dt *DisableTable) ToProto() (proto.Message, error) {
	return &pb.DisableTableRequest{
		TableName: &pb.TableName{
			// TODO: handle namespaces
			Namespace: []byte("default"),
			Qualifier: dt.table,
		},
	}, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (dt *DisableTable) NewResponse() proto.Message {
	return &pb.DisableTableResponse{}
}
