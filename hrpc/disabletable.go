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

type DisableTable struct {
	base
}

func NewDisableTable(ctx context.Context, table []byte) *DisableTable {
	dt := &DisableTable{
		base{
			table: table,
			ctx:   ctx,
		},
	}
	return dt
}

func (dt *DisableTable) GetName() string {
	return "DisableTable"
}

func (dt *DisableTable) Serialize() ([]byte, error) {
	dtreq := &pb.DisableTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: dt.table,
		},
	}
	return proto.Marshal(dtreq)
}

func (dt *DisableTable) NewResponse() proto.Message {
	return &pb.DisableTableResponse{}
}

// SetFilter always returns an error when used on Mutate objects. Do not use.
// Exists solely so DisableTable can implement the Call interface.
func (dt *DisableTable) SetFilter(ft filter.Filter) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set filter on disable table operation.")
}

// SetFamilies always returns an error when used on Mutate objects. Do not use.
// Exists solely so DisableTable can implement the Call interface.
func (dt *DisableTable) SetFamilies(fam map[string][]string) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set families on disable table operation.")
}
