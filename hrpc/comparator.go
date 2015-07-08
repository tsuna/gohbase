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

// BitComparatorBitwiseOp is TODO
type BitComparatorBitwiseOp int32

func (o BitComparatorBitwiseOp) isValid() bool {
	return (o >= 1 && o <= 3)
}

// Constants are TODO
const (
	BitComparatorAND BitComparatorBitwiseOp = 1
	BitComparatorOR  BitComparatorBitwiseOp = 2
	BitComparatorXOR BitComparatorBitwiseOp = 3
)

// Ensure our types implement Comparator correctly.
var _ Comparator = (*BinaryComparator)(nil)
var _ Comparator = (*LongComparator)(nil)
var _ Comparator = (*BinaryPrefixComparator)(nil)
var _ Comparator = (*BitComparator)(nil)
var _ Comparator = (*NullComparator)(nil)
var _ Comparator = (*RegexStringComparator)(nil)
var _ Comparator = (*SubstringComparator)(nil)

// Comparator is TODO
type Comparator interface {
	// ConstructPBComparator creates and returns the comparator encoded in a pb.Comparator type
	ConstructPBComparator() (*pb.Comparator, error)
}

// ByteArrayComparable is used across many Comparators. Implementing here to
// avoid users having to interface with protobuf generated files.
type ByteArrayComparable struct {
	Value []byte
}

// NewByteArrayComparable is TODO
func NewByteArrayComparable(value []byte) *ByteArrayComparable {
	return &ByteArrayComparable{
		Value: value,
	}
}

// ConstructPB is TODO
func (b *ByteArrayComparable) ConstructPB() *pb.ByteArrayComparable {
	pbVersion := &pb.ByteArrayComparable{
		Value: b.Value,
	}
	return pbVersion
}

// BinaryComparator is TODO
type BinaryComparator struct {
	Name       string
	Comparable *ByteArrayComparable
}

// NewBinaryComparator is TODO
func NewBinaryComparator(comparable *ByteArrayComparable) *BinaryComparator {
	return &BinaryComparator{
		Name:       comparatorPath + "BinaryComparator",
		Comparable: comparable,
	}
}

// ConstructPBComparator is TODO
func (c *BinaryComparator) ConstructPBComparator() (*pb.Comparator, error) {
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

// LongComparator is TODO
type LongComparator struct {
	Name       string
	Comparable *ByteArrayComparable
}

// NewLongComparator is TODO
func NewLongComparator(comparable *ByteArrayComparable) *LongComparator {
	return &LongComparator{
		Name:       comparatorPath + "LongComparator",
		Comparable: comparable,
	}
}

// ConstructPBComparator is TODO
func (c *LongComparator) ConstructPBComparator() (*pb.Comparator, error) {
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

// BinaryPrefixComparator is TODO
type BinaryPrefixComparator struct {
	Name       string
	Comparable *ByteArrayComparable
}

// NewBinaryPrefixComparator is TODO
func NewBinaryPrefixComparator(comparable *ByteArrayComparable) *BinaryPrefixComparator {
	return &BinaryPrefixComparator{
		Name:       comparatorPath + "BinaryPrefixComparator",
		Comparable: comparable,
	}
}

// ConstructPBComparator is TODO
func (c *BinaryPrefixComparator) ConstructPBComparator() (*pb.Comparator, error) {
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

// BitComparator is TODO
type BitComparator struct {
	Name       string
	Comparable *ByteArrayComparable
	BitwiseOp  BitComparatorBitwiseOp
}

// NewBitComparator is TODO
func NewBitComparator(bitwiseOp BitComparatorBitwiseOp, comparable *ByteArrayComparable) *BitComparator {
	return &BitComparator{
		Name:       comparatorPath + "BitComparator",
		Comparable: comparable,
		BitwiseOp:  bitwiseOp,
	}
}

// ConstructPBComparator is TODO
func (c *BitComparator) ConstructPBComparator() (*pb.Comparator, error) {
	if !c.BitwiseOp.isValid() {
		return nil, errors.New("Invalid bitwise operator specified")
	}
	b := pb.BitComparator_BitwiseOp(c.BitwiseOp)
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

// NullComparator is TODO
type NullComparator struct {
	Name string
}

// NewNullComparator is TODO
func NewNullComparator() *NullComparator {
	return &NullComparator{
		Name: comparatorPath + "NullComparator",
	}
}

// ConstructPBComparator is TODO
func (c *NullComparator) ConstructPBComparator() (*pb.Comparator, error) {
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

// RegexStringComparator is TODO
type RegexStringComparator struct {
	Name         string
	Pattern      string
	PatternFlags int32
	Charset      string
	Engine       string
}

// NewRegexStringComparator is TODO
func NewRegexStringComparator(pattern string, patternFlags int32,
	charset, engine string) *RegexStringComparator {
	return &RegexStringComparator{
		Name:         comparatorPath + "RegexStringComparator",
		Pattern:      pattern,
		PatternFlags: patternFlags,
		Charset:      charset,
		Engine:       engine,
	}
}

// ConstructPBComparator is TODO
func (c *RegexStringComparator) ConstructPBComparator() (*pb.Comparator, error) {
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

// SubstringComparator is TODO
type SubstringComparator struct {
	Name   string
	Substr string
}

// NewSubstringComparator is TODO
func NewSubstringComparator(substr string) *SubstringComparator {
	return &SubstringComparator{
		Name:   comparatorPath + "SubstringComparator",
		Substr: substr,
	}
}

// ConstructPBComparator is TODO
func (c *SubstringComparator) ConstructPBComparator() (*pb.Comparator, error) {
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
