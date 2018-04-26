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

// DeleteTable represents a DeleteTable HBase call
type DeleteTable struct {
	base
}

// NewDeleteTable creates a new DeleteTable request that will delete the
// given table in HBase. For use by the admin client.
func NewDeleteTable(ctx context.Context, table []byte) *DeleteTable {
	return &DeleteTable{
		base{
			table:    table,
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
	}
}

// Name returns the name of this RPC call.
func (dt *DeleteTable) Name() string {
	return "DeleteTable"
}

// ToProto converts the RPC into a protobuf message
func (dt *DeleteTable) ToProto() proto.Message {
	namespace := []byte("default")
	table := dt.table
	i := bytes.Index(table, []byte(":"))
	if i > -1 {
		namespace = table[:i]
		table = table[i+1:]
	}

	return &pb.DeleteTableRequest{
		TableName: &pb.TableName{
			Namespace: namespace,
			Qualifier: table,
		},
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (dt *DeleteTable) NewResponse() proto.Message {
	return &pb.DeleteTableResponse{}
}
