// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/internal/pb"
	"golang.org/x/net/context"
)

// GetProcedureState represents a call to HBase to check status of a procedure
type GetProcedureState struct {
	base

	procID uint64
}

// NewGetProcedureState creates a new GetProcedureState request. For use by the admin client.
func NewGetProcedureState(ctx context.Context, procID uint64) *GetProcedureState {
	return &GetProcedureState{
		base: base{
			ctx: ctx,
		},
		procID: procID,
	}
}

// Name returns the name of this RPC call.
func (ps *GetProcedureState) Name() string {
	return "getProcedureResult"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (ps *GetProcedureState) Serialize() ([]byte, error) {
	req := &pb.GetProcedureResultRequest{
		ProcId: &ps.procID,
	}
	return proto.Marshal(req)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ps *GetProcedureState) NewResponse() proto.Message {
	return &pb.GetProcedureResultResponse{}
}

// SetFilter always returns an error.
func (ps *GetProcedureState) SetFilter(filter.Filter) error {
	// Doesn't make sense on this kind of RPC.
	return errors.New("cannot set filter on admin operations")
}

// SetFamilies always returns an error.
func (ps *GetProcedureState) SetFamilies(map[string][]string) error {
	// Doesn't make sense on this kind of RPC.
	return errors.New("cannot set families on admin operations")
}
