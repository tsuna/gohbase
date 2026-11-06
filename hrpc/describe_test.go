// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"testing"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func TestNewGetTableDescriptors(t *testing.T) {
	ctx := context.Background()

	// no options — defaults
	d, err := NewGetTableDescriptors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.Name() != "GetTableDescriptors" {
		t.Errorf("expected name GetTableDescriptors, got %s", d.Name())
	}
	if d.Description() != "GetTableDescriptors" {
		t.Errorf("expected description GetTableDescriptors, got %s", d.Description())
	}
	if d.Context() != ctx {
		t.Error("context mismatch")
	}

	// with all options
	names := []*pb.TableName{
		{Namespace: []byte("ns"), Qualifier: []byte("tbl")},
	}
	d, err = NewGetTableDescriptors(ctx,
		DescribeTableNames(names),
		DescribeRegex("test.*"),
		DescribeNamespace("ns"),
		DescribeSysTables(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.tableNames) != 1 {
		t.Errorf("expected 1 table name, got %d", len(d.tableNames))
	}
	if d.regex != "test.*" {
		t.Errorf("expected regex test.*, got %s", d.regex)
	}
	if d.namespace != "ns" {
		t.Errorf("expected namespace ns, got %s", d.namespace)
	}
	if !d.includeSysTables {
		t.Error("expected includeSysTables to be true")
	}
}

func TestGetTableDescriptorsToProto(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		opts     []func(Call) error
		expProto *pb.GetTableDescriptorsRequest
	}{
		{
			name: "defaults",
			expProto: &pb.GetTableDescriptorsRequest{
				IncludeSysTables: proto.Bool(false),
			},
		},
		{
			name: "with regex",
			opts: []func(Call) error{DescribeRegex("test.*")},
			expProto: &pb.GetTableDescriptorsRequest{
				Regex:            proto.String("test.*"),
				IncludeSysTables: proto.Bool(false),
			},
		},
		{
			name: "with namespace",
			opts: []func(Call) error{DescribeNamespace("myns")},
			expProto: &pb.GetTableDescriptorsRequest{
				Namespace:        proto.String("myns"),
				IncludeSysTables: proto.Bool(false),
			},
		},
		{
			name: "with sys tables",
			opts: []func(Call) error{DescribeSysTables(true)},
			expProto: &pb.GetTableDescriptorsRequest{
				IncludeSysTables: proto.Bool(true),
			},
		},
		{
			name: "with table names",
			opts: []func(Call) error{
				DescribeTableNames([]*pb.TableName{
					{Namespace: []byte("default"), Qualifier: []byte("t1")},
					{Namespace: []byte("default"), Qualifier: []byte("t2")},
				}),
			},
			expProto: &pb.GetTableDescriptorsRequest{
				TableNames: []*pb.TableName{
					{Namespace: []byte("default"), Qualifier: []byte("t1")},
					{Namespace: []byte("default"), Qualifier: []byte("t2")},
				},
				IncludeSysTables: proto.Bool(false),
			},
		},
		{
			name: "all options",
			opts: []func(Call) error{
				DescribeTableNames([]*pb.TableName{
					{Namespace: []byte("ns"), Qualifier: []byte("tbl")},
				}),
				DescribeRegex("tbl.*"),
				DescribeNamespace("ns"),
				DescribeSysTables(true),
			},
			expProto: &pb.GetTableDescriptorsRequest{
				TableNames: []*pb.TableName{
					{Namespace: []byte("ns"), Qualifier: []byte("tbl")},
				},
				Regex:            proto.String("tbl.*"),
				IncludeSysTables: proto.Bool(true),
				Namespace:        proto.String("ns"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := NewGetTableDescriptors(ctx, tt.opts...)
			if err != nil {
				t.Fatal(err)
			}
			p := d.ToProto()
			out, ok := p.(*pb.GetTableDescriptorsRequest)
			if !ok {
				t.Fatalf("expected *pb.GetTableDescriptorsRequest, got %T", p)
			}
			if !proto.Equal(out, tt.expProto) {
				t.Errorf("expected %v, got %v", tt.expProto, out)
			}
		})
	}
}

func TestGetTableDescriptorsNewResponse(t *testing.T) {
	ctx := context.Background()
	d, err := NewGetTableDescriptors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resp := d.NewResponse()
	if _, ok := resp.(*pb.GetTableDescriptorsResponse); !ok {
		t.Fatalf("expected *pb.GetTableDescriptorsResponse, got %T", resp)
	}
}

func TestDescribeOptionsWrongType(t *testing.T) {
	ctx := context.Background()
	get, err := NewGet(ctx, []byte("t"), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}

	opts := []struct {
		name string
		opt  func(Call) error
	}{
		{"DescribeTableNames", DescribeTableNames([]*pb.TableName{
			{Namespace: []byte("ns"), Qualifier: []byte("t")},
		})},
		{"DescribeRegex", DescribeRegex(".*")},
		{"DescribeNamespace", DescribeNamespace("ns")},
		{"DescribeSysTables", DescribeSysTables(true)},
	}

	for _, o := range opts {
		t.Run(o.name, func(t *testing.T) {
			if err := o.opt(get); err == nil {
				t.Errorf("expected error applying %s to Get, got nil", o.name)
			}
		})
	}
}

func TestDescribeTableNamesEmpty(t *testing.T) {
	ctx := context.Background()

	_, err := NewGetTableDescriptors(ctx, DescribeTableNames(nil))
	if err == nil {
		t.Error("expected error for nil table names, got nil")
	}

	_, err = NewGetTableDescriptors(ctx, DescribeTableNames([]*pb.TableName{}))
	if err == nil {
		t.Error("expected error for empty table names, got nil")
	}
}
