// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

// Mutate represents a mutation on HBase.
type Mutate struct {
	base

	row          *[]byte
	mutationType pb.MutationProto_MutationType //*int32

	//values is a map of column families to a map of column qualifiers to bytes
	values map[string]map[string][]byte
}

// NewPutStr creates a new Mutation request that will put the given values into
// HBase under the given table and key.
func NewPutStr(table, key string, values map[string]map[string][]byte) *Mutate {
	return &Mutate{
		base: base{
			table: []byte(table),
			key:   []byte(key),
		},
		mutationType: pb.MutationProto_PUT,
		values:       values,
	}
}

// NewDelStr creates a new Mutation request that will delete the given values
// from HBase under the given table and key.
func NewDelStr(table, key string, values map[string]map[string][]byte) *Mutate {
	return &Mutate{
		base: base{
			table: []byte(table),
			key:   []byte(key),
		},
		mutationType: pb.MutationProto_DELETE,
		values:       values,
	}
}

// NewAppStr creates a new Mutation request that will append the given values
// to their existing values in HBase under the given table and key.
func NewAppStr(table, key string, values map[string]map[string][]byte) *Mutate {
	return &Mutate{
		base: base{
			table: []byte(table),
			key:   []byte(key),
		},
		mutationType: pb.MutationProto_APPEND,
		values:       values,
	}
}

// NewIncStr creates a new Mutation request that will increment the given values
// in HBase under the given table and key.
func NewIncStr(table, key string, values map[string]map[string][]byte) *Mutate {
	return &Mutate{
		base: base{
			table: []byte(table),
			key:   []byte(key),
		},
		mutationType: pb.MutationProto_INCREMENT,
		values:       values,
	}
}

// Name returns the name of this RPC call.
func (m *Mutate) Name() string {
	return "Mutate"
}

// Serialize converts this mutate object into a protobuf message suitable for
// sending to an HBase server
func (m *Mutate) Serialize() ([]byte, error) {
	// We need to convert everything in the values field
	// to a protobuf ColumnValue
	bytevalues := make([]*pb.MutationProto_ColumnValue, len(m.values))
	counter := 0
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
		bytevalues[counter] = &pb.MutationProto_ColumnValue{
			Family:         []byte(k),
			QualifierValue: qualvals,
		}
		counter++
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
