// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

var (
	// ErrNotAStruct is returned by any of the *Ref functions when something
	// other than a struct is passed in to their data argument
	ErrNotAStruct = errors.New("data must be a struct")

	// ErrUnsupportedUints is returned when this message is serialized and uints
	// are unsupported on your platform (this will probably never happen)
	ErrUnsupportedUints = errors.New("uints are unsupported on your platform")

	// ErrUnsupportedInts is returned when this message is serialized and ints
	// are unsupported on your platform (this will probably never happen)
	ErrUnsupportedInts = errors.New("ints are unsupported on your platform")

	attributeNameTTL = "_ttl"
)

// DurabilityType is used to set durability for Durability option
type DurabilityType int32

const (
	// UseDefault is USER_DEFAULT
	UseDefault DurabilityType = iota
	// SkipWal is SKIP_WAL
	SkipWal
	// AsyncWal is ASYNC_WAL
	AsyncWal
	// SyncWal is SYNC_WAL
	SyncWal
	// FsyncWal is FSYNC_WAL
	FsyncWal
)

// Mutate represents a mutation on HBase.
type Mutate struct {
	base

	mutationType pb.MutationProto_MutationType //*int32

	// values is a map of column families to a map of column qualifiers to bytes
	values map[string]map[string][]byte

	ttl        []byte
	timestamp  uint64
	durability DurabilityType
	skipbatch  bool
}

// TTL sets a time to live for mutation queries.
func TTL(t time.Duration) func(Call) error {
	return func(o Call) error {
		m, ok := o.(*Mutate)
		if !ok {
			return errors.New("'TTL' option can only be used with mutation queries")
		}

		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(t.Seconds()*1000))

		m.ttl = buf

		return nil
	}
}

// Timestamp sets timestamp for mutation queries.
// The time object passed will be rounded to a millisecond resolution, as by default,
// if no timestamp is provided, HBase sets it to current time in milliseconds.
// In order to have custom time precision, use TimestampUint64 call option for
// mutation requests and corresponding TimeRangeUint64 for retrieval requests.
func Timestamp(ts time.Time) func(Call) error {
	return func(o Call) error {
		m, ok := o.(*Mutate)
		if !ok {
			return errors.New("'Timestamp' option can only be used with mutation queries")
		}
		m.timestamp = uint64(ts.UnixNano() / 1e6)
		return nil
	}
}

// TimestampUint64 sets timestamp for mutation queries.
func TimestampUint64(ts uint64) func(Call) error {
	return func(o Call) error {
		m, ok := o.(*Mutate)
		if !ok {
			return errors.New("'TimestampUint64' option can only be used with mutation queries")
		}
		m.timestamp = ts
		return nil
	}
}

// Durability sets durability for mutation queries.
func Durability(d DurabilityType) func(Call) error {
	return func(o Call) error {
		m, ok := o.(*Mutate)
		if !ok {
			return errors.New("'Durability' option can only be used with mutation queries")
		}
		if d < UseDefault || d > FsyncWal {
			return errors.New("invalid durability value")
		}
		m.durability = d
		return nil
	}
}

// baseMutate returns a Mutate struct without the mutationType filled in.
func baseMutate(ctx context.Context, table, key string, values map[string]map[string][]byte,
	options ...func(Call) error) (*Mutate, error) {
	m := &Mutate{
		base: base{
			table:    []byte(table),
			key:      []byte(key),
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		values:    values,
		timestamp: MaxTimestamp,
	}
	err := applyOptions(m, options...)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// NewPutStr creates a new Mutation request to insert the given
// family-column-values in the given row key of the given table.
func NewPutStr(ctx context.Context, table, key string,
	values map[string]map[string][]byte, options ...func(Call) error) (*Mutate, error) {
	m, err := baseMutate(ctx, table, key, values, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_PUT
	return m, nil
}

// NewDelStr creates a new Mutation request to delete the given
// family-column-values from the given row key of the given table.
func NewDelStr(ctx context.Context, table, key string,
	values map[string]map[string][]byte, options ...func(Call) error) (*Mutate, error) {
	m, err := baseMutate(ctx, table, key, values, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_DELETE
	return m, nil
}

// NewAppStr creates a new Mutation request to append the given
// family-column-values into the existing cells in HBase (or create them if
// needed), in given row key of the given table.
func NewAppStr(ctx context.Context, table, key string,
	values map[string]map[string][]byte, options ...func(Call) error) (*Mutate, error) {
	m, err := baseMutate(ctx, table, key, values, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_APPEND
	return m, nil
}

// NewIncStrSingle creates a new Mutation request that will increment the given value
// by amount in HBase under the given table, key, family and qualifier.
func NewIncStrSingle(ctx context.Context, table, key string, family string,
	qualifier string, amount int64, options ...func(Call) error) (*Mutate, error) {

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, amount)
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %s", err)
	}

	value := map[string]map[string][]byte{family: map[string][]byte{qualifier: buf.Bytes()}}
	return NewIncStr(ctx, table, key, value, options...)
}

// NewIncStr creates a new Mutation request that will increment the given values
// in HBase under the given table and key.
func NewIncStr(ctx context.Context, table, key string,
	values map[string]map[string][]byte, options ...func(Call) error) (*Mutate, error) {
	m, err := baseMutate(ctx, table, key, values, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_INCREMENT
	return m, nil
}

// Name returns the name of this RPC call.
func (m *Mutate) Name() string {
	return "Mutate"
}

// SkipBatch returns true if the Mutate request shouldn't be batched,
// but should be sent to Region Server right away.
func (m *Mutate) SkipBatch() bool {
	return m.skipbatch
}

func (m *Mutate) setSkipBatch(v bool) {
	m.skipbatch = v
}

func (m *Mutate) toProto() *pb.MutateRequest {
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
	durability := pb.MutationProto_Durability(m.durability)
	mProto := &pb.MutationProto{
		Row:         m.key,
		MutateType:  &m.mutationType,
		ColumnValue: bytevalues,
		Durability:  &durability,
	}
	if m.timestamp != MaxTimestamp {
		mProto.Timestamp = &m.timestamp
	}

	if len(m.ttl) > 0 {
		mProto.Attribute = append(mProto.Attribute, &pb.NameBytesPair{
			Name:  &attributeNameTTL,
			Value: m.ttl,
		})
	}

	return &pb.MutateRequest{
		Region:   m.regionSpecifier(),
		Mutation: mProto,
	}
}

// ToProto converts this mutate RPC into a protobuf message
func (m *Mutate) ToProto() proto.Message {
	return m.toProto()
}

// NewResponse creates an empty protobuf message to read the response of this RPC.
func (m *Mutate) NewResponse() proto.Message {
	return &pb.MutateResponse{}
}

// DeserializeCellBlocks deserializes mutate result from cell blocks
func (m *Mutate) DeserializeCellBlocks(pm proto.Message, b []byte) (uint32, error) {
	resp := pm.(*pb.MutateResponse)
	if resp.Result == nil {
		// TODO: is this possible?
		return 0, nil
	}
	cells, read, err := deserializeCellBlocks(b, uint32(resp.Result.GetAssociatedCellCount()))
	if err != nil {
		return 0, err
	}
	resp.Result.Cell = append(resp.Result.Cell, cells...)
	return read, nil
}
