// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/tsuna/gohbase/pb"
)

func TestNewCheckAndMutate(t *testing.T) {
	ctx := context.Background()
	table := []byte("test-table")
	key := []byte("test-key")
	values := map[string]map[string][]byte{
		"cf": {"col": []byte("value")},
	}

	t.Run("ValidInput", func(t *testing.T) {
		mutate, err := NewPut(ctx, table, key, values)
		if err != nil {
			t.Fatalf("Failed to create mutate: %v", err)
		}

		cam, err := NewCheckAndMutate(mutate, "cf", "col", []byte("expected"), pb.CompareType_EQUAL)
		if err != nil {
			t.Fatalf("NewCheckAndMutate failed: %v", err)
		}

		if cam == nil {
			t.Fatal("CheckAndMutate should not be nil")
		}

		if string(cam.family) != "cf" {
			t.Errorf("Expected family 'cf', got '%s'", string(cam.family))
		}

		if string(cam.qualifier) != "col" {
			t.Errorf("Expected qualifier 'col', got '%s'", string(cam.qualifier))
		}

		if *cam.compareType != pb.CompareType_EQUAL {
			t.Errorf("Expected compareType EQUAL, got %v", *cam.compareType)
		}

		if cam.comparator == nil {
			t.Error("Comparator should not be nil")
		}

		// Verify that skipBatch is set to true
		if !mutate.skipbatch {
			t.Error("Expected skipbatch to be true for CheckAndMutate")
		}
	})

	t.Run("NilMutate", func(t *testing.T) {
		_, err := NewCheckAndMutate(nil, "cf", "col", []byte("expected"), pb.CompareType_EQUAL)
		if err == nil {
			t.Error("Expected error for nil mutate")
		}
		if err.Error() != "mutate cannot be nil" {
			t.Errorf("Expected 'mutate cannot be nil', got '%s'", err.Error())
		}
	})

	t.Run("EmptyFamily", func(t *testing.T) {
		mutate, err := NewPut(ctx, table, key, values)
		if err != nil {
			t.Fatalf("Failed to create mutate: %v", err)
		}

		_, err = NewCheckAndMutate(mutate, "", "col", []byte("expected"), pb.CompareType_EQUAL)
		if err == nil {
			t.Error("Expected error for empty family")
		}
		if err.Error() != "family cannot be empty" {
			t.Errorf("Expected 'family cannot be empty', got '%s'", err.Error())
		}
	})

	t.Run("EmptyQualifier", func(t *testing.T) {
		mutate, err := NewPut(ctx, table, key, values)
		if err != nil {
			t.Fatalf("Failed to create mutate: %v", err)
		}

		_, err = NewCheckAndMutate(mutate, "cf", "", []byte("expected"), pb.CompareType_EQUAL)
		if err == nil {
			t.Error("Expected error for empty qualifier")
		}
		if err.Error() != "qualifier cannot be empty" {
			t.Errorf("Expected 'qualifier cannot be empty', got '%s'", err.Error())
		}
	})

	t.Run("DifferentCompareTypes", func(t *testing.T) {
		mutate, err := NewPut(ctx, table, key, values)
		if err != nil {
			t.Fatalf("Failed to create mutate: %v", err)
		}

		compareTypes := []pb.CompareType{
			pb.CompareType_EQUAL,
			pb.CompareType_NOT_EQUAL,
			pb.CompareType_LESS,
			pb.CompareType_LESS_OR_EQUAL,
			pb.CompareType_GREATER,
			pb.CompareType_GREATER_OR_EQUAL,
		}

		for _, ct := range compareTypes {
			cam, err := NewCheckAndMutate(mutate, "cf", "col", []byte("expected"), ct)
			if err != nil {
				t.Errorf("NewCheckAndMutate failed for compareType %v: %v", ct, err)
				continue
			}

			if *cam.compareType != ct {
				t.Errorf("Expected compareType %v, got %v", ct, *cam.compareType)
			}
		}
	})

	t.Run("NilExpectedValue", func(t *testing.T) {
		mutate, err := NewPut(ctx, table, key, values)
		if err != nil {
			t.Fatalf("Failed to create mutate: %v", err)
		}

		cam, err := NewCheckAndMutate(mutate, "cf", "col", nil, pb.CompareType_EQUAL)
		if err != nil {
			t.Fatalf("NewCheckAndMutate failed with nil expectedValue: %v", err)
		}

		if cam == nil {
			t.Fatal("CheckAndMutate should not be nil")
		}
	})

	t.Run("EmptyExpectedValue", func(t *testing.T) {
		mutate, err := NewPut(ctx, table, key, values)
		if err != nil {
			t.Fatalf("Failed to create mutate: %v", err)
		}

		cam, err := NewCheckAndMutate(mutate, "cf", "col", []byte{}, pb.CompareType_EQUAL)
		if err != nil {
			t.Fatalf("NewCheckAndMutate failed with empty expectedValue: %v", err)
		}

		if cam == nil {
			t.Fatal("CheckAndMutate should not be nil")
		}
	})
}

func TestCheckAndMutateToProto(t *testing.T) {
	ctx := context.Background()
	table := []byte("test-table")
	key := []byte("test-key")
	values := map[string]map[string][]byte{
		"cf": {"col": []byte("value")},
	}

	mutate, err := NewPut(ctx, table, key, values)
	if err != nil {
		t.Fatalf("Failed to create mutate: %v", err)
	}

	cam, err := NewCheckAndMutate(mutate, "cf", "col", []byte("expected"), pb.CompareType_NOT_EQUAL)
	if err != nil {
		t.Fatalf("NewCheckAndMutate failed: %v", err)
	}

	// Set region before calling ToProto
	cam.SetRegion(mockRegionInfo([]byte("test-region")))

	protoMsg := cam.ToProto()
	mutateRequest, ok := protoMsg.(*pb.MutateRequest)
	if !ok {
		t.Fatalf("Expected *pb.MutateRequest, got %T", protoMsg)
	}

	if mutateRequest.Condition == nil {
		t.Fatal("Condition should not be nil")
	}

	condition := mutateRequest.Condition

	if !bytes.Equal(condition.Row, key) {
		t.Errorf("Expected row %v, got %v", key, condition.Row)
	}

	if !bytes.Equal(condition.Family, []byte("cf")) {
		t.Errorf("Expected family 'cf', got %v", condition.Family)
	}

	if !bytes.Equal(condition.Qualifier, []byte("col")) {
		t.Errorf("Expected qualifier 'col', got %v", condition.Qualifier)
	}

	if *condition.CompareType != pb.CompareType_NOT_EQUAL {
		t.Errorf("Expected compareType NOT_EQUAL, got %v", *condition.CompareType)
	}

	if condition.Comparator == nil {
		t.Error("Comparator should not be nil")
	}

	// Verify the comparator is a BinaryComparator
	if condition.Comparator.Name == nil {
		t.Error("Comparator name should not be nil")
	} else if *condition.Comparator.Name != "org.apache.hadoop.hbase.filter.BinaryComparator" {
		t.Errorf("Expected BinaryComparator, got %s", *condition.Comparator.Name)
	}
}

func TestCheckAndMutateCellBlocksEnabled(t *testing.T) {
	ctx := context.Background()
	table := []byte("test-table")
	key := []byte("test-key")
	values := map[string]map[string][]byte{
		"cf": {"col": []byte("value")},
	}

	mutate, err := NewPut(ctx, table, key, values)
	if err != nil {
		t.Fatalf("Failed to create mutate: %v", err)
	}

	cam, err := NewCheckAndMutate(mutate, "cf", "col", []byte("expected"), pb.CompareType_EQUAL)
	if err != nil {
		t.Fatalf("NewCheckAndMutate failed: %v", err)
	}

	if cam.CellBlocksEnabled() {
		t.Error("CellBlocksEnabled should return false for CheckAndMutate")
	}
}

func TestCheckAndMutateWithDifferentMutationTypes(t *testing.T) {
	ctx := context.Background()
	table := []byte("test-table")
	key := []byte("test-key")
	values := map[string]map[string][]byte{
		"cf": {"col": []byte("value")},
	}

	testCases := []struct {
		name        string
		createMutate func() (*Mutate, error)
	}{
		{
			name: "Put",
			createMutate: func() (*Mutate, error) {
				return NewPut(ctx, table, key, values)
			},
		},
		{
			name: "Delete",
			createMutate: func() (*Mutate, error) {
				return NewDel(ctx, table, key, values)
			},
		},
		{
			name: "Append",
			createMutate: func() (*Mutate, error) {
				return NewApp(ctx, table, key, values)
			},
		},
		{
			name: "Increment",
			createMutate: func() (*Mutate, error) {
				return NewInc(ctx, table, key, values)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mutate, err := tc.createMutate()
			if err != nil {
				t.Fatalf("Failed to create %s mutate: %v", tc.name, err)
			}

			cam, err := NewCheckAndMutate(mutate, "cf", "col", []byte("expected"), pb.CompareType_EQUAL)
			if err != nil {
				t.Fatalf("NewCheckAndMutate failed for %s: %v", tc.name, err)
			}

			if cam == nil {
				t.Fatalf("CheckAndMutate should not be nil for %s", tc.name)
			}

			// Verify that the underlying mutate type is preserved
			if cam.Mutate.mutationType != mutate.mutationType {
				t.Errorf("Expected mutation type %v, got %v", mutate.mutationType, cam.Mutate.mutationType)
			}
		})
	}
}
