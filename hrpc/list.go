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

// ListTableNamesByNamespace represents a ListTableNamesByNamespace HBase call
type ListTableNamesByNamespace struct {
	base
	namespace string
}

// NewListTableNamesByNamespace creates a new ListTableNamesByNamespace request that will list the
// tables for the given HBase namespace
func NewListTableNamesByNamespace(ctx context.Context, namespace string) *ListTableNamesByNamespace {
	return &ListTableNamesByNamespace{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		namespace: namespace,
	}
}

// Name returns the name of this RPC call.
func (lt *ListTableNamesByNamespace) Name() string {
	return "ListTableNamesByNamespace"
}

// ToProto converts the RPC into a protobuf message
func (lt *ListTableNamesByNamespace) ToProto() proto.Message {
	return &pb.ListTableNamesByNamespaceRequest{
		NamespaceName: &lt.namespace,
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (lt *ListTableNamesByNamespace) NewResponse() proto.Message {
	return &pb.ListTableNamesByNamespaceResponse{}
}
