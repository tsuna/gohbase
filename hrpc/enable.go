// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/internal/pb"
	"golang.org/x/net/context"
)

// EnableTable represents a EnableTable HBase call
type EnableTable struct {
	tableOp
}

// NewEnableTable creates a new EnableTable request that will enable the
// given table in HBase. For use by the admin client.
func NewEnableTable(ctx context.Context, table []byte) *EnableTable {
	et := &EnableTable{
		tableOp{base{
			table: table,
			ctx:   ctx,
		}},
	}
	return et
}

// Name returns the name of this RPC call.
func (et *EnableTable) Name() string {
	return "EnableTable"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (et *EnableTable) Serialize() ([]byte, error) {
	dtreq := &pb.EnableTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: et.table,
		},
	}
	return proto.Marshal(dtreq)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (et *EnableTable) NewResponse() proto.Message {
	return &pb.EnableTableResponse{}
}
