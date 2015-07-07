// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

const comparatorPath = "org.apache.hadoop.hbase.filter."

// Ensure our types implement Comparator correctly.
var _ Comparator = (*BinaryComparator)(nil)
var _ Comparator = (*LongComparator)(nil)
var _ Comparator = (*BinaryPrefixComparator)(nil)
var _ Comparator = (*BitComparator)(nil)
var _ Comparator = (*NullComparator)(nil)
var _ Comparator = (*RegexStringComparator)(nil)
var _ Comparator = (*SubstringComparator)(nil)

type Comparator interface {
	// ConstructPBComparator creates and returns the comparator encoded in a pb.Comparator type
	ConstructPBComparator() (*pb.Comparator, error)
}

// Comparable is used across many Comparators. Implementing here to avoid users having to
// interface with protobuf generated files.
type ByteArrayComparable struct {
	Value []byte
}

func NewByteArrayComparable(value []byte) *ByteArrayComparable {
	return &ByteArrayComparable{
		Value: value,
	}
}

func (b ByteArrayComparable) ConstructPB() *pb.ByteArrayComparable {
	pbVersion := &pb.ByteArrayComparable{
		Value: b.Value,
	}
	return pbVersion
}

// BinaryComparator
type BinaryComparator struct {
	Name       string
	Comparable *ByteArrayComparable
}

func NewBinaryComparator(comparable *ByteArrayComparable) *BinaryComparator {
	return &BinaryComparator{
		Name:       comparatorPath + "BinaryComparator",
		Comparable: comparable,
	}
}

func (c BinaryComparator) ConstructPBComparator() (*pb.Comparator, error) {
	internalComparator := &pb.BinaryComparator{
		Comparable: c.Comparable.ConstructPB(),
	}
	serializedComparator, err := proto.Marshal(internalComparator)
	if err != nil {
		return nil, err
	}
	comparator := &pb.Comparator{
		Name:                 &c.Name,
		SerializedComparator: serializedComparator,
	}
	return comparator, nil
}

// LongComparator
type LongComparator struct {
	Name       string
	Comparable *ByteArrayComparable
}

func NewLongComparator(comparable *ByteArrayComparable) *LongComparator {
	return &LongComparator{
		Name:       comparatorPath + "LongComparator",
		Comparable: comparable,
	}
}

func (c LongComparator) ConstructPBComparator() (*pb.Comparator, error) {
	internalComparator := &pb.LongComparator{
		Comparable: c.Comparable.ConstructPB(),
	}
	serializedComparator, err := proto.Marshal(internalComparator)
	if err != nil {
		return nil, err
	}
	comparator := &pb.Comparator{
		Name:                 &c.Name,
		SerializedComparator: serializedComparator,
	}
	return comparator, nil
}

// BinaryPrefixComparator
type BinaryPrefixComparator struct {
	Name       string
	Comparable *ByteArrayComparable
}

func NewBinaryPrefixComparator(comparable *ByteArrayComparable) *BinaryPrefixComparator {
	return &BinaryPrefixComparator{
		Name:       comparatorPath + "BinaryPrefixComparator",
		Comparable: comparable,
	}
}

func (c BinaryPrefixComparator) ConstructPBComparator() (*pb.Comparator, error) {
	internalComparator := &pb.BinaryPrefixComparator{
		Comparable: c.Comparable.ConstructPB(),
	}
	serializedComparator, err := proto.Marshal(internalComparator)
	if err != nil {
		return nil, err
	}
	comparator := &pb.Comparator{
		Name:                 &c.Name,
		SerializedComparator: serializedComparator,
	}
	return comparator, nil
}

// BitComparator
type BitComparator struct {
	Name       string
	Comparable *ByteArrayComparable
	BitwiseOp  string
}

func NewBitComparator(bitwiseOp string, comparable *ByteArrayComparable) *BitComparator {
	return &BitComparator{
		Name:       comparatorPath + "BitComparator",
		Comparable: comparable,
		BitwiseOp:  bitwiseOp,
	}
}

func (c BitComparator) ConstructPBComparator() (*pb.Comparator, error) {
	b := pb.BitComparator_BitwiseOp(1)
	if val, ok := pb.BitComparator_BitwiseOp_value[c.BitwiseOp]; ok {
		b = pb.BitComparator_BitwiseOp(val)
	} else {
		return nil, errors.New("Invalid bitwise operation specified.")
	}
	internalComparator := &pb.BitComparator{
		Comparable: c.Comparable.ConstructPB(),
		BitwiseOp:  &b,
	}
	serializedComparator, err := proto.Marshal(internalComparator)
	if err != nil {
		return nil, err
	}
	comparator := &pb.Comparator{
		Name:                 &c.Name,
		SerializedComparator: serializedComparator,
	}
	return comparator, nil
}

// NullComparator
type NullComparator struct {
	Name string
}

func NewNullComparator() *NullComparator {
	return &NullComparator{
		Name: comparatorPath + "NullComparator",
	}
}

func (c NullComparator) ConstructPBComparator() (*pb.Comparator, error) {
	internalComparator := &pb.NullComparator{}
	serializedComparator, err := proto.Marshal(internalComparator)
	if err != nil {
		return nil, err
	}
	comparator := &pb.Comparator{
		Name:                 &c.Name,
		SerializedComparator: serializedComparator,
	}
	return comparator, nil
}

// RegexStringComparator
type RegexStringComparator struct {
	Name         string
	Pattern      string
	PatternFlags int32
	Charset      string
	Engine       string
}

func NewRegexStringComparator(pattern string, patternFlags int32, charset, engine string) *RegexStringComparator {
	return &RegexStringComparator{
		Name:         comparatorPath + "RegexStringComparator",
		Pattern:      pattern,
		PatternFlags: patternFlags,
		Charset:      charset,
		Engine:       engine,
	}
}

func (c RegexStringComparator) ConstructPBComparator() (*pb.Comparator, error) {
	internalComparator := &pb.RegexStringComparator{
		Pattern:      &c.Pattern,
		PatternFlags: &c.PatternFlags,
		Charset:      &c.Charset,
		Engine:       &c.Engine,
	}
	serializedComparator, err := proto.Marshal(internalComparator)
	if err != nil {
		return nil, err
	}
	comparator := &pb.Comparator{
		Name:                 &c.Name,
		SerializedComparator: serializedComparator,
	}
	return comparator, nil
}

// SubstringComparator
type SubstringComparator struct {
	Name   string
	Substr string
}

func NewSubstringComparator(substr string) *SubstringComparator {
	return &SubstringComparator{
		Name:   comparatorPath + "SubstringComparator",
		Substr: substr,
	}
}

func (c SubstringComparator) ConstructPBComparator() (*pb.Comparator, error) {
	internalComparator := &pb.SubstringComparator{
		Substr: &c.Substr,
	}
	serializedComparator, err := proto.Marshal(internalComparator)
	if err != nil {
		return nil, err
	}
	comparator := &pb.Comparator{
		Name:                 &c.Name,
		SerializedComparator: serializedComparator,
	}
	return comparator, nil
}
