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

func TestNewModifyTable(t *testing.T) {
	ctx := context.Background()
	table := []byte("test_table")
	schema := &pb.TableSchema{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: table,
		},
		ColumnFamilies: []*pb.ColumnFamilySchema{
			{Name: []byte("cf1")},
		},
	}

	mt, err := NewModifyTable(ctx, table, schema)
	if err != nil {
		t.Fatal(err)
	}

	if mt.Name() != "ModifyTable" {
		t.Errorf("expected name ModifyTable, got %s", mt.Name())
	}
	if mt.Description() != "ModifyTable" {
		t.Errorf("expected description ModifyTable, got %s", mt.Description())
	}
	if mt.Context() != ctx {
		t.Error("context mismatch")
	}
	if mt.schema != schema {
		t.Error("schema mismatch")
	}
}

func TestModifyTableToProto(t *testing.T) {
	ctx := context.Background()
	table := []byte("test_table")
	schema := &pb.TableSchema{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: table,
		},
		ColumnFamilies: []*pb.ColumnFamilySchema{
			{Name: []byte("cf1")},
			{Name: []byte("cf2")},
		},
	}

	mt, err := NewModifyTable(ctx, table, schema)
	if err != nil {
		t.Fatal(err)
	}
	p := mt.ToProto()
	out, ok := p.(*pb.ModifyTableRequest)
	if !ok {
		t.Fatalf("expected *pb.ModifyTableRequest, got %T", p)
	}

	expProto := &pb.ModifyTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: table,
		},
		TableSchema: schema,
	}

	if !proto.Equal(out, expProto) {
		t.Errorf("expected %v, got %v", expProto, out)
	}

	// non-default namespace: ToProto should use the schema's namespace
	schemaCustomNS := &pb.TableSchema{
		TableName: &pb.TableName{
			Namespace: []byte("prod"),
			Qualifier: table,
		},
		ColumnFamilies: []*pb.ColumnFamilySchema{
			{Name: []byte("cf1")},
		},
	}
	mt2, err := NewModifyTable(ctx, table, schemaCustomNS)
	if err != nil {
		t.Fatal(err)
	}
	out2 := mt2.ToProto().(*pb.ModifyTableRequest)
	if string(out2.GetTableName().GetNamespace()) != "prod" {
		t.Errorf("expected namespace prod, got %s",
			string(out2.GetTableName().GetNamespace()))
	}
}

func TestModifyTableNewResponse(t *testing.T) {
	ctx := context.Background()
	mt, err := NewModifyTable(ctx, []byte("t"), &pb.TableSchema{})
	if err != nil {
		t.Fatal(err)
	}
	resp := mt.NewResponse()
	if _, ok := resp.(*pb.ModifyTableResponse); !ok {
		t.Fatalf("expected *pb.ModifyTableResponse, got %T", resp)
	}
}

func TestModifyTableNilSchema(t *testing.T) {
	_, err := NewModifyTable(context.Background(), []byte("t"), nil)
	if err == nil {
		t.Error("expected error for nil schema, got nil")
	}
}
