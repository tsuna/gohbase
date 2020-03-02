// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

// GetTableNames represents a GetTableNames HBase call
type GetTableNames struct {
	base
	regex *string
}

// NewGetTableNames creates a new GetTableNames request that will get table
// names in HBase. For use by the admin client.
func NewGetTableNames(ctx context.Context, regex string) *GetTableNames {
	return &GetTableNames{
		base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		proto.String(regex),
	}
}

// Name returns the name of this RPC call.
func (gt *GetTableNames) Name() string {
	return "GetTableNames"
}

// ToProto converts the RPC into a protobuf message
func (gt *GetTableNames) ToProto() proto.Message {
	return &pb.GetTableNamesRequest{
		Regex: gt.regex,
		Namespace: proto.String("default"),
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (gt *GetTableNames) NewResponse() proto.Message {
	return &pb.GetTableNamesResponse{}
}

