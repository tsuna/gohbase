// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// CheckAndMutate performs a provided mutation if the condition is met.
// Unlike CheckAndPut, this supports any mutation type (Put, Delete, Append, Increment)
// and any comparison operator (LESS, LESS_OR_EQUAL, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, GREATER).
// It also supports filter-based conditions.
type CheckAndMutate struct {
	*Mutate

	family      []byte
	qualifier   []byte
	compareType pb.CompareType
	comparator  *pb.Comparator
	filter      *pb.Filter
}

// NewCheckAndMutate creates a new CheckAndMutate request that will compare the value
// at the specified family:qualifier using the given comparator and comparison operator.
// If the condition is met, the provided mutation will be applied.
func NewCheckAndMutate(
	mutation *Mutate,
	family string,
	qualifier string,
	compareType pb.CompareType,
	comparator filter.Comparator,
) (*CheckAndMutate, error) {
	cmp, err := comparator.ConstructPBComparator()
	if err != nil {
		return nil, err
	}

	// CheckAndMutate is not batchable as MultiResponse doesn't return Processed field
	// for Mutate Action
	mutation.setSkipBatch(true)

	return &CheckAndMutate{
		Mutate:      mutation,
		family:      []byte(family),
		qualifier:   []byte(qualifier),
		compareType: compareType,
		comparator:  cmp,
	}, nil
}

// NewCheckAndMutateWithFilter creates a new CheckAndMutate request that uses the provided
// filter for row comparison instead of a CompareType and Comparator.
func NewCheckAndMutateWithFilter(
	mutation *Mutate,
	f filter.Filter,
) (*CheckAndMutate, error) {
	pbFilter, err := f.ConstructPBFilter()
	if err != nil {
		return nil, err
	}

	mutation.setSkipBatch(true)

	return &CheckAndMutate{
		Mutate: mutation,
		filter: pbFilter,
	}, nil
}

// ToProto converts the RPC into a protobuf message.
func (cam *CheckAndMutate) ToProto() proto.Message {
	mutateRequest, _, _ := cam.toProto(false, nil)
	condition := &pb.Condition{
		Row: cam.key,
	}

	if cam.filter != nil {
		condition.Filter = cam.filter
	} else {
		condition.Family = cam.family
		condition.Qualifier = cam.qualifier
		condition.CompareType = cam.compareType.Enum()
		condition.Comparator = cam.comparator
	}

	mutateRequest.Condition = condition
	return mutateRequest
}

// CellBlocksEnabled returns false because cellblocks are not supported for
// check and mutate requests.
func (cam *CheckAndMutate) CellBlocksEnabled() bool {
	return false
}
