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

// CheckAndPut performs a provided Put operation if the value specified
// by condition equals to the one set in the HBase.
type CheckAndPut struct {
	*Mutate

	family    []byte
	qualifier []byte

	compareType *pb.CompareType
	comparator  *pb.Comparator
}

// NewCheckAndPut creates a CheckAndPut with only an EQUAL compare type.
func NewCheckAndPut(put *Mutate, family string,
	qualifier string, expectedValue []byte) (*CheckAndPut, error) {
	if put.mutationType != pb.MutationProto_PUT {
		return nil, fmt.Errorf("'CheckAndPut' only takes 'Put' request")
	}
	return NewCheckAndPutWithCompareType(
		put, family, qualifier, expectedValue, pb.CompareType_EQUAL)
}

// NewCheckAndPutWithCompareType creates a new CheckAndPut request that will compare provided
// expectedValue with the on in HBase located at put's row and provided family:qualifier,
// and if they are equal, perform the provided put request on the row
func NewCheckAndPutWithCompareType(put *Mutate, family string,
	qualifier string, expectedValue []byte, compareType pb.CompareType) (*CheckAndPut, error) {

	// The condition that needs to match for the edit to be applied.
	exp := filter.NewByteArrayComparable(expectedValue)
	cmp, err := filter.NewBinaryComparator(exp).ConstructPBComparator()
	if err != nil {
		return nil, err
	}

	// CheckAndPut is not batchable as MultiResponse doesn't return Processed field
	// for Mutate Action
	put.setSkipBatch(true)

	return &CheckAndPut{
		Mutate:      put,
		family:      []byte(family),
		qualifier:   []byte(qualifier),
		compareType: &compareType,
		comparator:  cmp,
	}, nil
}

// ToProto converts the RPC into a protobuf message
func (cp *CheckAndPut) ToProto() proto.Message {
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

func (cp *CheckAndPut) CellBlocksEnabled() bool {
	// cellblocks are not supported for check and put request
	return false
}
