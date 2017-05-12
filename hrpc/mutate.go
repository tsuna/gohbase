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
	"io"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
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

	// data is a struct passed in that has fields tagged to represent HBase
	// columns
	data interface{}

	// timestamp to save at
	timestamp uint64

	// mutation durability
	durability DurabilityType
}

// Timestamp sets timestamp for mutation queries.
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
	data interface{}, options ...func(Call) error) (*Mutate, error) {
	m := &Mutate{
		base: base{
			table: []byte(table),
			key:   []byte(key),
			ctx:   ctx,
		},
		values:    values,
		data:      data,
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
	m, err := baseMutate(ctx, table, key, values, nil, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_PUT
	return m, nil
}

// NewPutStrRef creates a new Mutation request to insert the given
// data structure in the given row key of the given table.  The `data'
// argument must be a string with fields defined using the "hbase" tag.
func NewPutStrRef(ctx context.Context, table, key string, data interface{},
	options ...func(Call) error) (*Mutate, error) {
	if !isAStruct(data) {
		return nil, ErrNotAStruct
	}
	m, err := baseMutate(ctx, table, key, nil, data, options...)
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
	m, err := baseMutate(ctx, table, key, values, nil, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_DELETE
	return m, nil
}

// NewDelStrRef creates a new Mutation request to delete the given
// data structure from the given row key of the given table.  The `data'
// argument must be a string with fields defined using the "hbase" tag.
func NewDelStrRef(ctx context.Context, table, key string, data interface{},
	options ...func(Call) error) (*Mutate, error) {
	if !isAStruct(data) {
		return nil, ErrNotAStruct
	}
	m, err := baseMutate(ctx, table, key, nil, data, options...)
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
	m, err := baseMutate(ctx, table, key, values, nil, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_APPEND
	return m, nil
}

// NewAppStrRef creates a new Mutation request that will append the given values
// to their existing values in HBase under the given table and key.
func NewAppStrRef(ctx context.Context, table, key string, data interface{},
	options ...func(Call) error) (*Mutate, error) {
	if !isAStruct(data) {
		return nil, ErrNotAStruct
	}
	m, err := baseMutate(ctx, table, key, nil, data, options...)
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
	m, err := baseMutate(ctx, table, key, values, nil, options...)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_INCREMENT
	return m, nil
}

// NewIncStrRef creates a new Mutation request that will increment the given values
// in HBase under the given table and key.
func NewIncStrRef(ctx context.Context, table, key string, data interface{},
	options ...func(Call) error) (*Mutate, error) {
	if !isAStruct(data) {
		return nil, ErrNotAStruct
	}
	m, err := baseMutate(ctx, table, key, nil, data, options...)
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

// ToProto converts this mutate RPC into a protobuf message
func (m *Mutate) ToProto() (proto.Message, error) {
	return m.toProto()
}

func (m *Mutate) toProto() (*pb.MutateRequest, error) {
	if m.data == nil {
		return m.serializeNoReflect(), nil
	}
	return m.serializeWithReflect()
}

func (m *Mutate) serializeNoReflect() *pb.MutateRequest {
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
	return &pb.MutateRequest{
		Region:   m.regionSpecifier(),
		Mutation: mProto,
	}
}

// serializeWithReflect is a helper function for Serialize. It is used when
// there is a struct with tagged fields to be serialized.
func (m *Mutate) serializeWithReflect() (*pb.MutateRequest, error) {
	typeOf := reflect.TypeOf(m.data)
	valueOf := reflect.Indirect(reflect.ValueOf(m.data))

	columns := make(map[string][]*pb.MutationProto_ColumnValue_QualifierValue)

	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		if field.PkgPath != "" {
			// This is an unexported field of the struct, so we're going to
			// ignore it
			continue
		}

		tagval := field.Tag.Get("hbase")
		if tagval == "" {
			// If the tag is empty, we're going to ignore this field
			continue
		}
		cnames := strings.SplitN(tagval, ":", 2)
		if len(cnames) != 2 {
			// If the tag doesn't contain a colon, it's set improperly
			return nil, fmt.Errorf("invalid column family and column qualifier: \"%s\"", cnames)
		}
		cfamily := cnames[0]
		cqualifier := cnames[1]

		binaryValue, err := valueToBytes(valueOf.Field(i))
		if err != nil {
			return nil, err
		}

		qualVal := &pb.MutationProto_ColumnValue_QualifierValue{
			Qualifier: []byte(cqualifier),
			Value:     binaryValue,
		}

		if m.mutationType == pb.MutationProto_DELETE {
			tmp := pb.MutationProto_DELETE_MULTIPLE_VERSIONS
			qualVal.DeleteType = &tmp
		}
		columns[cfamily] = append(columns[cfamily], qualVal)
	}

	pbcolumns := make([]*pb.MutationProto_ColumnValue, 0, len(columns))
	for k, v := range columns {
		colval := &pb.MutationProto_ColumnValue{
			Family:         []byte(k),
			QualifierValue: v,
		}
		pbcolumns = append(pbcolumns, colval)

	}
	durability := pb.MutationProto_Durability(m.durability)
	mProto := &pb.MutationProto{
		Row:         m.key,
		MutateType:  &m.mutationType,
		ColumnValue: pbcolumns,
		Durability:  &durability,
	}
	if m.timestamp != MaxTimestamp {
		mProto.Timestamp = &m.timestamp
	}
	return &pb.MutateRequest{
		Region:   m.regionSpecifier(),
		Mutation: mProto,
	}, nil
}

// valueToBytes will convert a given value from the reflect package into its
// underlying bytes
func valueToBytes(val reflect.Value) ([]byte, error) {
	switch val.Kind() {
	case reflect.Bool:
		if val.Bool() {
			return []byte{1}, nil
		}
		return []byte{0}, nil

	case reflect.Uint:
		switch unsafe.Sizeof(unsafe.Pointer(val.UnsafeAddr())) {
		case 8:
			var x uint8
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		case 16:
			var x uint16
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		case 32:
			var x uint32
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		case 64:
			var x uint64
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		default:
			return nil, ErrUnsupportedUints
		}

	case reflect.Int:
		switch unsafe.Sizeof(unsafe.Pointer(val.UnsafeAddr())) {
		case 8:
			var x uint8
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		case 16:
			var x uint16
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		case 32:
			var x uint32
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		case 64:
			var x uint64
			return valueToBytes(val.Convert(reflect.TypeOf(x)))
		default:
			return nil, ErrUnsupportedInts
		}

	case reflect.Int8:
		var x int8
		x = val.Interface().(int8)
		memory := (*(*[1]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil
	case reflect.Uint8:
		var x uint8
		x = val.Interface().(uint8)
		memory := (*(*[1]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil

	case reflect.Int16:
		var x int16
		x = val.Interface().(int16)
		memory := (*(*[2]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil
	case reflect.Uint16:
		var x uint16
		x = val.Interface().(uint16)
		memory := (*(*[2]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil

	case reflect.Int32:
		var x int32
		x = val.Interface().(int32)
		memory := (*(*[4]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil
	case reflect.Uint32:
		var x uint32
		x = val.Interface().(uint32)
		memory := (*(*[4]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil
	case reflect.Float32:
		var x float32
		x = val.Interface().(float32)
		memory := (*(*[4]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil

	case reflect.Int64:
		var x int64
		x = val.Interface().(int64)
		memory := (*(*[8]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil
	case reflect.Uint64:
		var x uint64
		x = val.Interface().(uint64)
		memory := (*(*[8]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil
	case reflect.Float64:
		var x float64
		x = val.Interface().(float64)
		memory := (*(*[8]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil
	case reflect.Complex64:
		var x complex64
		x = val.Interface().(complex64)
		memory := (*(*[8]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil

	case reflect.Complex128:
		var x complex128
		x = val.Interface().(complex128)
		memory := (*(*[16]byte)(unsafe.Pointer(&x)))[:]
		return copyOf(memory), nil

	case reflect.Ptr:
		return valueToBytes(val.Elem())

	case reflect.Array, reflect.Slice:
		if val.Len() == 0 {
			return []byte{}, nil
		}
		kind := val.Index(0).Kind()
		if kind == reflect.Array || kind == reflect.Slice || kind == reflect.String {
			// We won't be able to deserialize this later into the correct types, since
			// arrays/slices/strings don't have a defined size.
			return nil, fmt.Errorf("slices and arrays of type %s is unsupported",
				val.Index(0).Type().Name())
		}
		var allbytes []byte
		for i := 0; i < val.Len(); i++ {
			morebytes, err := valueToBytes(val.Index(i))
			if err != nil {
				return nil, err
			}
			allbytes = append(allbytes, morebytes...)
		}
		return allbytes, nil

	case reflect.String:
		return []byte(val.String()), nil

		// Unhandled types, left here for easy reference
		//case reflect.Invalid:
		//case reflect.Chan:
		//case reflect.Func:
		//case reflect.Interface:
		//case reflect.Struct:
		//case reflect.Map:
		//case reflect.Uintptr:
		//case reflect.UnsafePointer:
	}
	return nil, fmt.Errorf("unsupported type %s, %d", val.Type().Name(), val.Kind())
}

func copyOf(memory []byte) []byte {
	memcpy := make([]byte, len(memory))
	copy(memcpy, memory)
	return memcpy
}

func isAStruct(data interface{}) bool {
	return reflect.TypeOf(data).Kind() == reflect.Struct
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (m *Mutate) NewResponse() proto.Message {
	return &pb.MutateResponse{}
}

// DeserializeCellBlocks deserializes mutate result from cell blocks
func (m *Mutate) DeserializeCellBlocks(pm proto.Message, r io.Reader, cellsLen uint32) error {
	resp := pm.(*pb.MutateResponse)
	if resp.Result == nil {
		// TODO: is this possible?
		return nil
	}
	cells, err := deserializeCellBlocks(r, cellsLen)
	if err != nil {
		return err
	}
	resp.Result.Cell = append(resp.Result.Cell, cells...)
	return nil
}

// SetFilter always returns an error when used on Mutate objects. Do not use.
// Exists solely so Mutate can implement the Call interface.
func (m *Mutate) SetFilter(ft filter.Filter) error {
	// Not allowed. Throw an error
	return errors.New("cannot call 'SetFilter' on mutate operation")
}

// SetFamilies always returns an error when used on Mutate objects. Do not use.
// Exists solely so Mutate can implement the Call interface.
func (m *Mutate) SetFamilies(fam map[string][]string) error {
	// Not allowed. Throw an error
	return errors.New("cannot call 'SetFamilies' on mutate operation")
}
