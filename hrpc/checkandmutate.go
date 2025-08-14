// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"fmt"

	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// CheckAndMutate performs a provided Mutate operation if the value specified
// by condition equals to the one set in the HBase.
type CheckAndMutate struct {
	*Mutate

	family    []byte
	qualifier []byte

	compareType *pb.CompareType
	comparator  *pb.Comparator
}

// NewCheckAndMutate creates a new CheckAndMutate request that will compare provided
// expectedValue with the on in HBase located at mutate's row and provided family:qualifier,
// and if the compare is, perform the provided mutate request on the row
func NewCheckAndMutate(mutate *Mutate, family string,
	qualifier string, expectedValue []byte, compareType pb.CompareType) (*CheckAndMutate, error) {

	if mutate == nil {
		return nil, fmt.Errorf("mutate cannot be nil")
	}

	if family == "" {
		return nil, fmt.Errorf("family cannot be empty")
	}

	if qualifier == "" {
		return nil, fmt.Errorf("qualifier cannot be empty")
	}

	// The condition that needs to match for the edit to be applied.
	exp := filter.NewByteArrayComparable(expectedValue)
	cmp, err := filter.NewBinaryComparator(exp).ConstructPBComparator()
	if err != nil {
		return nil, err
	}

	// CheckAndMutate is not batchable as MultiResponse doesn't return Processed field
	// for Mutate Action
	mutate.setSkipBatch(true)

	return &CheckAndMutate{
		Mutate:      mutate,
		family:      []byte(family),
		qualifier:   []byte(qualifier),
		compareType: &compareType,
		comparator:  cmp,
	}, nil
}

// ToProto converts the RPC into a protobuf message
func (cp *CheckAndMutate) ToProto() proto.Message {
	mutateRequest, _, _ := cp.toProto(false, nil)
	mutateRequest.Condition = &pb.Condition{
		Row:         cp.key,
		Family:      cp.family,
		Qualifier:   cp.qualifier,
		CompareType: cp.compareType,
		Comparator:  cp.comparator,
	}
	return mutateRequest
}

func (cp *CheckAndMutate) CellBlocksEnabled() bool {
	// cellblocks are not supported for check and mutate request
	return false
}
