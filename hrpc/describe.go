// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// GetTableDescriptors models a GetTableDescriptors pb call
type GetTableDescriptors struct {
	base
	tableNames       []*pb.TableName
	regex            string
	includeSysTables bool
	namespace        string
}

// DescribeTableNames sets an explicit list of tables to retrieve descriptors
// for. When set, only the schemas for the given tables are returned.
func DescribeTableNames(names []*pb.TableName) func(Call) error {
	return func(c Call) error {
		d, ok := c.(*GetTableDescriptors)
		if !ok {
			return errors.New(
				"DescribeTableNames option can only be used with GetTableDescriptors")
		}
		if len(names) == 0 {
			return errors.New(
				"DescribeTableNames requires at least one table name")
		}
		d.tableNames = names
		return nil
	}
}

// DescribeRegex sets a regular expression to filter tables by name. Only
// tables whose names match the regex will have their descriptors returned.
func DescribeRegex(regex string) func(Call) error {
	return func(c Call) error {
		d, ok := c.(*GetTableDescriptors)
		if !ok {
			return errors.New(
				"DescribeRegex option can only be used with GetTableDescriptors")
		}
		d.regex = regex
		return nil
	}
}

// DescribeNamespace restricts the request to tables within the given HBase
// namespace. If not set, tables from all namespaces are considered.
func DescribeNamespace(ns string) func(Call) error {
	return func(c Call) error {
		d, ok := c.(*GetTableDescriptors)
		if !ok {
			return errors.New(
				"DescribeNamespace option can only be used with GetTableDescriptors")
		}
		d.namespace = ns
		return nil
	}
}

// DescribeSysTables includes sys tables for GetTableDescriptors
func DescribeSysTables(b bool) func(Call) error {
	return func(c Call) error {
		d, ok := c.(*GetTableDescriptors)
		if !ok {
			return errors.New(
				"DescribeSysTables option can only be used with GetTableDescriptors")
		}
		d.includeSysTables = b
		return nil
	}
}

// NewGetTableDescriptors creates a new GetTableDescriptors request that will
// return table schemas from hbase.
//
// By default, matches all tables. Use the options (DescribeTableNames,
// DescribeRegex, DescribeNamespace, DescribeSysTables) to set non default
// behaviour.
func NewGetTableDescriptors(ctx context.Context,
	opts ...func(Call) error) (*GetTableDescriptors, error) {
	d := &GetTableDescriptors{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
	}
	if err := applyOptions(d, opts...); err != nil {
		return nil, err
	}
	return d, nil
}

// Name returns the name of this RPC call.
func (d *GetTableDescriptors) Name() string {
	return "GetTableDescriptors"
}

// Description returns the description of this RPC call.
func (d *GetTableDescriptors) Description() string {
	return d.Name()
}

// ToProto converts the RPC into a protobuf message.
func (d *GetTableDescriptors) ToProto() proto.Message {
	req := &pb.GetTableDescriptorsRequest{
		TableNames:       d.tableNames,
		IncludeSysTables: proto.Bool(d.includeSysTables),
	}
	if d.regex != "" {
		req.Regex = proto.String(d.regex)
	}
	if d.namespace != "" {
		req.Namespace = proto.String(d.namespace)
	}
	return req
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (d *GetTableDescriptors) NewResponse() proto.Message {
	return &pb.GetTableDescriptorsResponse{}
}
