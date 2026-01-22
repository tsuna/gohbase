// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func TestCheckAndMutateWithFilter(t *testing.T) {
	ctx := context.Background()
	table := []byte("table")
	key := []byte("key")
	rs := &pb.RegionSpecifier{
		Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
		Value: []byte("region"),
	}
	cf := "cf"
	qualifier := "q"

	tests := []struct {
		name     string
		mutation func() (*Mutate, error)
		filter   filter.Filter
		wantErr  bool
	}{
		{
			name: "put with value filter",
			mutation: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					cf: {qualifier: []byte("value")},
				})
			},
			filter: filter.NewValueFilter(filter.NewCompareFilter(
				filter.Equal,
				filter.NewBinaryComparator(filter.NewByteArrayComparable([]byte("expected"))))),
		},
		{
			name: "delete with column prefix filter",
			mutation: func() (*Mutate, error) {
				return NewDel(ctx, table, key, map[string]map[string][]byte{
					cf: {qualifier: nil},
				})
			},
			filter: filter.NewColumnPrefixFilter([]byte("prefix")),
		},
		{
			name: "multiple qualifiers in same row with filter list",
			mutation: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					cf: {"new_qualifier": []byte("new_value")},
				})
			},
			filter: filter.NewList(filter.MustPassAll,
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("qualifier1"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("expected1"))),
					false, false),
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("qualifier2"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("expected2"))),
					false, false),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutation, err := tt.mutation()
			if err != nil {
				t.Fatal(err)
			}

			cam, err := NewCheckAndMutateWithFilter(mutation, tt.filter)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			if cam.Name() != "Mutate" {
				t.Errorf("expected name 'Mutate', got %q", cam.Name())
			}

			if !cam.SkipBatch() {
				t.Error("expected SkipBatch to be true")
			}

			if cam.CellBlocksEnabled() {
				t.Error("expected CellBlocksEnabled to be false")
			}

			cam.SetRegion(mockRegionInfo("region"))
			req := cam.ToProto().(*pb.MutateRequest)

			if req.Condition == nil {
				t.Fatal("expected Condition to be set")
			}

			if !bytes.Equal(req.Condition.Row, key) {
				t.Errorf("expected row %q, got %q", key, req.Condition.Row)
			}

			if req.Condition.Filter == nil {
				t.Error("expected Filter to be set")
			}

			if req.Condition.Family != nil {
				t.Error("expected Family to be nil when using filter")
			}
			if req.Condition.Qualifier != nil {
				t.Error("expected Qualifier to be nil when using filter")
			}
			if req.Condition.Comparator != nil {
				t.Error("expected Comparator to be nil when using filter")
			}

			if !proto.Equal(req.Region, rs) {
				t.Errorf("expected region %v, got %v", rs, req.Region)
			}
		})
	}
}

// TestCheckAndMutateWithFilter_SameRowManyQualifiers tests multiple updates
// to a single row with different column qualifiers.
//
// This tests that CheckAndMutate with a Filter can check multiple keys in one operation.
func TestCheckAndMutateWithFilter_SameRowManyQualifiers(t *testing.T) {
	ctx := context.Background()
	table := []byte("table_for_filter_test")
	cf := "cf"
	key := []byte("someKey")

	tests := []struct {
		name     string
		mutation func() (*Mutate, error)
		filter   filter.Filter
	}{
		{
			name: "check single qualifier exists before update",
			mutation: func() (*Mutate, error) {
				// Update a new qualifier in the same row
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					cf: {"key3": []byte("value3")},
				})
			},
			filter: filter.NewSingleColumnValueFilter(
				[]byte(cf), []byte("key1"),
				filter.Equal,
				filter.NewBinaryComparator(
					filter.NewByteArrayComparable([]byte("expected_value1"))),
				false, false),
		},
		{
			name: "check multiple qualifiers with AND logic (MustPassAll) - success",
			mutation: func() (*Mutate, error) {
				// Update multiple qualifiers in the same row
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					cf: {
						"key4": []byte("value4"),
						"key5": []byte("value5"),
					},
				})
			},
			filter: filter.NewList(filter.MustPassAll,
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key1"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("v1"))),
					false, false),
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key2"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("v2"))),
					false, false),
			),
		},
		{
			name: "check any qualifier matches with OR logic (MustPassOne)",
			mutation: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					cf: {"status": []byte("processed")},
				})
			},
			filter: filter.NewList(filter.MustPassOne,
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key1"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("ready"))),
					false, false),
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key2"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("ready"))),
					false, false),
			),
		},
		{
			name: "check qualifier exists using column prefix",
			mutation: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					cf: {"processed:key1": []byte("done")},
				})
			},
			filter: filter.NewColumnPrefixFilter([]byte("pending:")),
		},
		{
			name: "check version count before update",
			mutation: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					cf: {"key1": []byte("new_value")},
				})
			},
			filter: filter.NewColumnCountGetFilter(5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutation, err := tt.mutation()
			if err != nil {
				t.Fatal(err)
			}

			cam, err := NewCheckAndMutateWithFilter(mutation, tt.filter)
			if err != nil {
				t.Fatal(err)
			}

			if !cam.SkipBatch() {
				t.Error("expected SkipBatch to be true")
			}

			cam.SetRegion(mockRegionInfo("region"))
			req := cam.ToProto().(*pb.MutateRequest)

			if req.Condition == nil {
				t.Fatal("expected Condition to be set")
			}

			if !bytes.Equal(req.Condition.Row, key) {
				t.Errorf("expected row %q, got %q", key, req.Condition.Row)
			}

			if req.Condition.Filter == nil {
				t.Error("expected Filter to be set")
			}
		})
	}
}
