// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

// Mutate represents a mutation on HBase.
type Mutate struct {
	base

	row          *[]byte
	mutationType pb.MutationProto_MutationType //*int32

	//values is a map of column families to a map of column qualifiers to bytes
	values map[string]map[string][]byte
}

// baseMutate will return a Mutate struct without the mutationType filled in.
func baseMutate(ctx context.Context, table, key string, values map[string]map[string][]byte) *Mutate {
	return &Mutate{
		base: base{
			table: []byte(table),
			key:   []byte(key),
			ctx:   ctx,
		},
		values: values,
	}
}

// NewPutStr creates a new Mutation request that will put the given values into
// HBase under the given table and key.
func NewPutStr(ctx context.Context, table, key string, values map[string]map[string][]byte) (*Mutate, error) {
	m := baseMutate(ctx, table, key, values)
	m.mutationType = pb.MutationProto_PUT
	return m, nil
}

// NewDelStr creates a new Mutation request that will delete the given values
// from HBase under the given table and key.
func NewDelStr(ctx context.Context, table, key string, values map[string]map[string][]byte) (*Mutate, error) {
	m := baseMutate(ctx, table, key, values)
	m.mutationType = pb.MutationProto_DELETE
	return m, nil
}

// NewAppStr creates a new Mutation request that will append the given values
// to their existing values in HBase under the given table and key.
func NewAppStr(ctx context.Context, table, key string, values map[string]map[string][]byte) (*Mutate, error) {
	m := baseMutate(ctx, table, key, values)
	m.mutationType = pb.MutationProto_APPEND
	return m, nil
}

// NewIncStr creates a new Mutation request that will increment the given values
// in HBase under the given table and key.
func NewIncStr(ctx context.Context, table, key string, values map[string]map[string][]byte) (*Mutate, error) {
	m := baseMutate(ctx, table, key, values)
	m.mutationType = pb.MutationProto_INCREMENT
	return m, nil
}

// GetName returns the name of this RPC call.
func (m *Mutate) GetName() string {
	return "Mutate"
}

// Serialize converts this mutate object into a protobuf message suitable for
// sending to an HBase server
func (m *Mutate) Serialize() ([]byte, error) {
	// We need to convert everything in the values field
	// to a protobuf ColumnValue
	bytevalues := make([]*pb.MutationProto_ColumnValue, len(m.values))
	i := 0
	for k, v := range m.values {
		qualvals := make([]*pb.MutationProto_ColumnValue_QualifierValue, len(v))
		j := 0
		// And likewise, each item in each column needs to be converted to a
		// protobuf QualifierValue
		for k1, v1 := range v {
			qualvals[j] = &pb.MutationProto_ColumnValue_QualifierValue{
				Qualifier: []byte(k1),
				Value:     v1,
			}
			if m.mutationType == pb.MutationProto_DELETE {
				tmp := pb.MutationProto_DELETE_MULTIPLE_VERSIONS
				qualvals[j].DeleteType = &tmp
			}
			j++
		}
		bytevalues[i] = &pb.MutationProto_ColumnValue{
			Family:         []byte(k),
			QualifierValue: qualvals,
		}
		i++
	}
	mutate := &pb.MutateRequest{
		Region: m.regionSpecifier(),
		Mutation: &pb.MutationProto{
			Row:         m.key,
			MutateType:  &m.mutationType,
			ColumnValue: bytevalues,
		},
	}
	return proto.Marshal(mutate)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (m *Mutate) NewResponse() proto.Message {
	return &pb.MutateResponse{}
}

// SetFilter always returns an error when used on Mutate objects. Do not use.
// Exists solely so Mutate can implement the Call interface.
func (m *Mutate) SetFilter(ft filter.Filter) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set filter on mutate operation.")
}

// SetFamilies always returns an error when used on Mutate objects. Do not use.
// Exists solely so Mutate can implement the Call interface.
func (m *Mutate) SetFamilies(fam map[string][]string) error {
	// Not allowed. Throw an error
	return errors.New("Cannot set families on mutate operation.")
}
