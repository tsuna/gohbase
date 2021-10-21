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

// EnableTable represents a EnableTable HBase call
type EnableTable struct {
	base
}

// NewEnableTable creates a new EnableTable request that will enable the
// given table in HBase. For use by the admin client.
func NewEnableTable(ctx context.Context, table []byte) *EnableTable {
	return &EnableTable{
		base{
			table:    table,
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
	}
}

// Name returns the name of this RPC call.
func (et *EnableTable) Name() string {
	return "EnableTable"
}

// Description returns the description of this RPC call.
func (et *EnableTable) Description() string {
	return et.Name()
}

// ToProto converts the RPC into a protobuf message
func (et *EnableTable) ToProto() proto.Message {
	return &pb.EnableTableRequest{
		TableName: &pb.TableName{
			// TODO: handle namespaces
			Namespace: []byte("default"),
			Qualifier: et.table,
		},
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (et *EnableTable) NewResponse() proto.Message {
	return &pb.EnableTableResponse{}
}
