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

type DeleteTable struct {
	base
}

func NewDeleteTable(ctx context.Context, table []byte) *DeleteTable {
	dt := &DeleteTable{
		base{
			table: table,
			ctx:   ctx,
		},
	}
	return dt
}

func (dt *DeleteTable) GetName() string {
	return "DeleteTable"
}

func (dt *DeleteTable) Serialize() ([]byte, error) {
	dtreq := &pb.DeleteTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: dt.table,
		},
	}
	return proto.Marshal(dtreq)
}

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
