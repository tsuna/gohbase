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

// DeleteTable represents a DeleteTable HBase call
type DeleteTable struct {
	base
}

// NewDeleteTable creates a new DeleteTable request that will delete the given
// table in HBase. For use by the admin client.
func NewDeleteTable(ctx context.Context, table []byte) *DeleteTable {
	dt := &DeleteTable{
		base{
			table: table,
			ctx:   ctx,
		},
	}
	return dt
}

// GetName returns the name of this RPC call.
func (dt *DeleteTable) GetName() string {
	return "DeleteTable"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (dt *DeleteTable) Serialize() ([]byte, error) {
	dtreq := &pb.DeleteTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: dt.table,
		},
	}
	return proto.Marshal(dtreq)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (dt *DeleteTable) NewResponse() proto.Message {
	return &pb.DeleteTableResponse{}
}

// SetFilter always returns an error when used on Mutate objects. Do not use.
// Exists solely so DeleteTable can implement the Call interface.
func (dt *DeleteTable) SetFilter(ft filter.Filter) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set filter on disable table operation.")
}

// SetFamilies always returns an error when used on Mutate objects. Do not use.
// Exists solely so DeleteTable can implement the Call interface.
func (dt *DeleteTable) SetFamilies(fam map[string][]string) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set families on disable table operation.")
}
