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

const filterPath = "org.apache.hadoop.hbase.filter."

// Ensure our types implement Filter correctly.
var _ Filter = (*FilterList)(nil)
var _ Filter = (*ColumnCountGetFilter)(nil)
var _ Filter = (*ColumnPaginationFilter)(nil)
var _ Filter = (*ColumnPrefixFilter)(nil)
var _ Filter = (*ColumnRangeFilter)(nil)
var _ Filter = (*CompareFilter)(nil)
var _ Filter = (*DependentColumnFilter)(nil)
var _ Filter = (*FamilyFilter)(nil)
var _ Filter = (*FilterWrapper)(nil)
var _ Filter = (*FirstKeyOnlyFilter)(nil)
var _ Filter = (*FirstKeyValueMatchingQualifiersFilter)(nil)
var _ Filter = (*FuzzyRowFilter)(nil)
var _ Filter = (*InclusiveStopFilter)(nil)
var _ Filter = (*KeyOnlyFilter)(nil)
var _ Filter = (*MultipleColumnPrefixFilter)(nil)
var _ Filter = (*PageFilter)(nil)
var _ Filter = (*PrefixFilter)(nil)
var _ Filter = (*QualifierFilter)(nil)
var _ Filter = (*RandomRowFilter)(nil)
var _ Filter = (*RowFilter)(nil)
var _ Filter = (*SingleColumnValueFilter)(nil)
var _ Filter = (*SingleColumnValueExcludeFilter)(nil)
var _ Filter = (*SkipFilter)(nil)
var _ Filter = (*TimestampsFilter)(nil)
var _ Filter = (*ValueFilter)(nil)
var _ Filter = (*WhileMatchFilter)(nil)
var _ Filter = (*FilterAllFilter)(nil)
var _ Filter = (*RowRange)(nil)
var _ Filter = (*MultiRowRangeFilter)(nil)

type Filter interface {
	// ConstructPBFilter creates and returns the filter encoded in a pb.Filter type
	//	- For most filters this just involves creating the special filter object,
	//	  serializing it, and then creating a standard Filter object with the name and
	//	  serialization inside.
	//	- For FilterLists this requires creating the protobuf FilterList which contains
	//	  an array []*pb.Filter (meaning we have to create, serialize, create all objects
	//	  in that array), serialize the newly created pb.FilterList and then create a
	//	  pb.Filter object containing that new serialization.
	ConstructPBFilter() (*pb.Filter, error)
}

// Exposing a type used in FuzzyRowFilter. Want to avoid users having to interact directly with
// the protobuf generated file so exposing here.
type BytesBytesPair struct {
	First  []byte
	Second []byte
}

func NewBytesBytesPair(first []byte, second []byte) *BytesBytesPair {
	return &BytesBytesPair{
		First:  first,
		Second: second,
	}
}

/*
    Each filter below has three primary methods/declarations, each of which can be summarized
    as follows -

	1. Type declaration. Create a new type for each filter. A 'Name' field is required but
	   you can create as many other fields as you like. These are purely local and will be
	   transcribed into a pb.Filter type by ConstructPBFilter()
	2. Constructor. Given a few parameters create the above type and return it to the callee.
	3. ConstructPBFilter. Take our local representation of a filter object and create the
	   appropriate pb.Filter object. Return the pb.Filter object.

	You may define any additional methods you like (see FilterList) but be aware that as soon
	as the returned object is type casted to a Filter (e.g. appending it to an array of Filters)
	it loses the ability to call those additional functions.
*/

// FilterList
type FilterList struct {
	Name     string
	Operator string
	Filters  []Filter
}

func NewFilterList(operator string, filters ...Filter) *FilterList {
	return &FilterList{
		Name:     filterPath + "FilterList",
		Operator: operator,
		Filters:  filters,
	}
}

func (f *FilterList) AddFilters(filters ...Filter) {
	f.Filters = append(f.Filters, filters...)
}

func (f FilterList) ConstructPBFilter() (*pb.Filter, error) {
	p := pb.FilterList_Operator(0)
	if val, ok := pb.FilterList_Operator_value[f.Operator]; ok {
		p = pb.FilterList_Operator(val)
	} else {
		return nil, errors.New("Invalid operator specified.")
	}

	filterArray := []*pb.Filter{}
	for i := 0; i < len(f.Filters); i++ {
		pbFilter, err := f.Filters[i].ConstructPBFilter()
		if err != nil {
			return nil, err
		}
		filterArray = append(filterArray, pbFilter)
	}
	filterList := &pb.FilterList{
		Operator: &p,
		Filters:  filterArray,
	}

	serializedFilter, err := proto.Marshal(filterList)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// ColumnCountGetFilter
type ColumnCountGetFilter struct {
	Name  string
	Limit int32
}

func NewColumnCountGetFilter(limit int32) *ColumnCountGetFilter {
	return &ColumnCountGetFilter{
		Name:  filterPath + "ColumnCountGetFilter",
		Limit: limit,
	}
}

func (f ColumnCountGetFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.ColumnCountGetFilter{
		Limit: &f.Limit,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// ColumnPaginationFilter
type ColumnPaginationFilter struct {
	Name         string
	Limit        int32
	Offset       int32
	ColumnOffset []byte
}

func NewColumnPaginationFilter(limit, offset int32, columnOffset []byte) *ColumnPaginationFilter {
	return &ColumnPaginationFilter{
		Name:         filterPath + "ColumnPaginationFilter",
		Limit:        limit,
		Offset:       offset,
		ColumnOffset: columnOffset,
	}
}

func (f ColumnPaginationFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.ColumnPaginationFilter{
		Limit:        &f.Limit,
		Offset:       &f.Offset,
		ColumnOffset: f.ColumnOffset,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// ColumnPrefixFilter
type ColumnPrefixFilter struct {
	Name   string
	Prefix []byte
}

func NewColumnPrefixFilter(prefix []byte) *ColumnPrefixFilter {
	return &ColumnPrefixFilter{
		Name:   filterPath + "ColumnPrefixFilter",
		Prefix: prefix,
	}
}

func (f ColumnPrefixFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.ColumnPrefixFilter{
		Prefix: f.Prefix,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// ColumnRangeFilter
type ColumnRangeFilter struct {
	Name               string
	MinColumn          []byte
	MinColumnInclusive bool
	MaxColumn          []byte
	MaxColumnInclusive bool
}

func NewColumnRangeFilter(minColumn, maxColumn []byte, minColumnInclusive, maxColumnInclusive bool) *ColumnRangeFilter {
	return &ColumnRangeFilter{
		Name:               filterPath + "ColumnRangeFilter",
		MinColumn:          minColumn,
		MaxColumn:          maxColumn,
		MinColumnInclusive: minColumnInclusive,
		MaxColumnInclusive: maxColumnInclusive,
	}
}

func (f ColumnRangeFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.ColumnRangeFilter{
		MinColumn:          f.MinColumn,
		MinColumnInclusive: &f.MinColumnInclusive,
		MaxColumn:          f.MaxColumn,
		MaxColumnInclusive: &f.MaxColumnInclusive,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// CompareFilter
type CompareFilter struct {
	Name          string
	CompareOp     string
	ComparatorObj Comparator
}

func NewCompareFilter(compareOp string, comparatorObj Comparator) *CompareFilter {
	return &CompareFilter{
		Name:          filterPath + "CompareFilter",
		CompareOp:     compareOp,
		ComparatorObj: comparatorObj,
	}
}

func (f CompareFilter) ConstructPB() (*pb.CompareFilter, error) {
	p := pb.CompareType(0)
	if val, ok := pb.CompareType_value[f.CompareOp]; ok {
		p = pb.CompareType(val)
	} else {
		return nil, errors.New("Invalid compare operation specified.")
	}
	pbComparatorObj, err := f.ComparatorObj.ConstructPBComparator()
	if err != nil {
		return nil, err
	}

	internalFilter := &pb.CompareFilter{
		CompareOp:  &p,
		Comparator: pbComparatorObj,
	}
	return internalFilter, nil
}

func (f CompareFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter, err := f.ConstructPB()
	if err != nil {
		return nil, err
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// DependentColumnFilter
type DependentColumnFilter struct {
	Name                string
	CompareFilterObj    CompareFilter
	ColumnFamily        []byte
	ColumnQualifier     []byte
	DropDependentColumn bool
}

func NewDependentColumnFilter(compareFilter CompareFilter, columnFamily, columnQualifier []byte, dropDependentColumn bool) *DependentColumnFilter {
	return &DependentColumnFilter{
		Name:                filterPath + "DependentColumnFilter",
		CompareFilterObj:    compareFilter,
		ColumnFamily:        columnFamily,
		ColumnQualifier:     columnQualifier,
		DropDependentColumn: dropDependentColumn,
	}
}

func (f DependentColumnFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbCompareFilterObj, err := f.CompareFilterObj.ConstructPB()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.DependentColumnFilter{
		CompareFilter:       pbCompareFilterObj,
		ColumnFamily:        f.ColumnFamily,
		ColumnQualifier:     f.ColumnQualifier,
		DropDependentColumn: &f.DropDependentColumn,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// FamilyFilter
type FamilyFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

func NewFamilyFilter(compareFilter CompareFilter) *FamilyFilter {
	return &FamilyFilter{
		Name:             filterPath + "FamilyFilter",
		CompareFilterObj: compareFilter,
	}
}

func (f FamilyFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbCompareFilterObj, err := f.CompareFilterObj.ConstructPB()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.FamilyFilter{
		CompareFilter: pbCompareFilterObj,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// FilterWrapper
type FilterWrapper struct {
	Name          string
	WrappedFilter Filter
}

func NewFilterWrapper(wrappedFilter Filter) *FilterWrapper {
	return &FilterWrapper{
		Name:          filterPath + "FilterWrapper",
		WrappedFilter: wrappedFilter,
	}
}

func (f FilterWrapper) ConstructPBFilter() (*pb.Filter, error) {
	wrappedFilter, err := f.WrappedFilter.ConstructPBFilter()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.FilterWrapper{
		Filter: wrappedFilter,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// FirstKeyOnlyFilter
type FirstKeyOnlyFilter struct {
	Name string
}

func NewFirstKeyOnlyFilter() *FirstKeyOnlyFilter {
	return &FirstKeyOnlyFilter{
		Name: filterPath + "FirstKeyOnlyFilter",
	}
}

func (f FirstKeyOnlyFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.FirstKeyOnlyFilter{}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// FirstKeyValueMatchingQualifiersFilter
type FirstKeyValueMatchingQualifiersFilter struct {
	Name       string
	Qualifiers [][]byte
}

func NewFirstKeyValueMatchingQualifiersFilter(qualifiers [][]byte) *FirstKeyValueMatchingQualifiersFilter {
	return &FirstKeyValueMatchingQualifiersFilter{
		Name:       filterPath + "FirstKeyValueMatchingQualifiersFilter",
		Qualifiers: qualifiers,
	}
}

func (f FirstKeyValueMatchingQualifiersFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.FirstKeyValueMatchingQualifiersFilter{
		Qualifiers: f.Qualifiers,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// FuzzyRowFilter
type FuzzyRowFilter struct {
	Name          string
	FuzzyKeysData []*BytesBytesPair
}

func NewFuzzyRowFilter(pairs []*BytesBytesPair) *FuzzyRowFilter {
	return &FuzzyRowFilter{
		Name:          filterPath + "FuzzyRowFilter",
		FuzzyKeysData: pairs,
	}
}

func (f FuzzyRowFilter) ConstructPBFilter() (*pb.Filter, error) {
	var pbFuzzyKeysData []*pb.BytesBytesPair
	for i := 0; i < len(f.FuzzyKeysData); i++ {
		pbPair := &pb.BytesBytesPair{
			First:  f.FuzzyKeysData[i].First,
			Second: f.FuzzyKeysData[i].Second,
		}
		pbFuzzyKeysData = append(pbFuzzyKeysData, pbPair)
	}

	internalFilter := &pb.FuzzyRowFilter{
		FuzzyKeysData: pbFuzzyKeysData,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// InclusiveStopFilter
type InclusiveStopFilter struct {
	Name       string
	StopRowKey []byte
}

func NewInclusiveStopFilter(stopRowKey []byte) *InclusiveStopFilter {
	return &InclusiveStopFilter{
		Name:       filterPath + "InclusiveStopFilter",
		StopRowKey: stopRowKey,
	}
}

func (f InclusiveStopFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.InclusiveStopFilter{
		StopRowKey: f.StopRowKey,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// KeyOnlyFilter
type KeyOnlyFilter struct {
	Name     string
	LenAsVal bool
}

func NewKeyOnlyFilter(lenAsVal bool) *KeyOnlyFilter {
	return &KeyOnlyFilter{
		Name:     filterPath + "KeyOnlyFilter",
		LenAsVal: lenAsVal,
	}
}

func (f KeyOnlyFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.KeyOnlyFilter{
		LenAsVal: &f.LenAsVal,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// MultipleColumnPrefixFilter
type MultipleColumnPrefixFilter struct {
	Name           string
	SortedPrefixes [][]byte
}

func NewMultipleColumnPrefixFilter(sortedPrefixes [][]byte) *MultipleColumnPrefixFilter {
	return &MultipleColumnPrefixFilter{
		Name:           filterPath + "MultipleColumnPrefixFilter",
		SortedPrefixes: sortedPrefixes,
	}
}

func (f MultipleColumnPrefixFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.MultipleColumnPrefixFilter{
		SortedPrefixes: f.SortedPrefixes,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// PageFilter
type PageFilter struct {
	Name     string
	PageSize int64
}

func NewPageFilter(pageSize int64) *PageFilter {
	return &PageFilter{
		Name:     filterPath + "PageFilter",
		PageSize: pageSize,
	}
}

func (f PageFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.PageFilter{
		PageSize: &f.PageSize,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// PrefixFilter
type PrefixFilter struct {
	Name   string
	Prefix []byte
}

func NewPrefixFilter(prefix []byte) *PrefixFilter {
	return &PrefixFilter{
		Name:   filterPath + "PrefixFilter",
		Prefix: prefix,
	}
}

func (f PrefixFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.PrefixFilter{
		Prefix: f.Prefix,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// QualifierFilter
type QualifierFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

func NewQualifierFilter(compareFilter CompareFilter) *QualifierFilter {
	return &QualifierFilter{
		Name:             filterPath + "QualifierFilter",
		CompareFilterObj: compareFilter,
	}
}

func (f QualifierFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbCompareFilterObj, err := f.CompareFilterObj.ConstructPB()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.QualifierFilter{
		CompareFilter: pbCompareFilterObj,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// RandomRowFilter
type RandomRowFilter struct {
	Name   string
	Chance float32
}

func NewRandomRowFilter(chance float32) *RandomRowFilter {
	return &RandomRowFilter{
		Name:   filterPath + "RandomRowFilter",
		Chance: chance,
	}
}

func (f RandomRowFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.RandomRowFilter{
		Chance: &f.Chance,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// RowFilter
type RowFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

func NewRowFilter(compareFilter CompareFilter) *RowFilter {
	return &RowFilter{
		Name:             filterPath + "RowFilter",
		CompareFilterObj: compareFilter,
	}
}

func (f RowFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbCompareFilterObj, err := f.CompareFilterObj.ConstructPB()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.RowFilter{
		CompareFilter: pbCompareFilterObj,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// SingleColumnValueFilter
type SingleColumnValueFilter struct {
	Name              string
	ColumnFamily      []byte
	ColumnQualifier   []byte
	CompareOp         string
	ComparatorObj     Comparator
	FilterIfMissing   bool
	LatestVersionOnly bool
}

func NewSingleColumnValueFilter(columnFamily, columnQualifier []byte, compareOp string, comparatorObj Comparator, filterIfMissing, latestVersionOnly bool) *SingleColumnValueFilter {
	return &SingleColumnValueFilter{
		Name:              filterPath + "SingleColumnValueFilter",
		ColumnFamily:      columnFamily,
		ColumnQualifier:   columnQualifier,
		CompareOp:         compareOp,
		ComparatorObj:     comparatorObj,
		FilterIfMissing:   filterIfMissing,
		LatestVersionOnly: latestVersionOnly,
	}
}

func (f SingleColumnValueFilter) ConstructPB() (*pb.SingleColumnValueFilter, error) {
	p := pb.CompareType(0)
	if val, ok := pb.CompareType_value[f.CompareOp]; ok {
		p = pb.CompareType(val)
	} else {
		return nil, errors.New("Invalid compare operation specified.")
	}
	pbComparatorObj, err := f.ComparatorObj.ConstructPBComparator()
	if err != nil {
		return nil, err
	}

	internalFilter := &pb.SingleColumnValueFilter{
		ColumnFamily:      f.ColumnFamily,
		ColumnQualifier:   f.ColumnQualifier,
		CompareOp:         &p,
		Comparator:        pbComparatorObj,
		FilterIfMissing:   &f.FilterIfMissing,
		LatestVersionOnly: &f.LatestVersionOnly,
	}
	return internalFilter, nil
}

func (f SingleColumnValueFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter, err := f.ConstructPB()
	if err != nil {
		return nil, err
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// SingleColumnValueExcludeFilter
type SingleColumnValueExcludeFilter struct {
	Name                       string
	SingleColumnValueFilterObj SingleColumnValueFilter
}

func NewSingleColumnValueExcludeFilter(filter SingleColumnValueFilter) *SingleColumnValueExcludeFilter {
	return &SingleColumnValueExcludeFilter{
		Name: filterPath + "SingleColumnValueExcludeFilter",
		SingleColumnValueFilterObj: filter,
	}
}

func (f SingleColumnValueExcludeFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbFilter, err := f.SingleColumnValueFilterObj.ConstructPB()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.SingleColumnValueExcludeFilter{
		SingleColumnValueFilter: pbFilter,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// SkipFilter
type SkipFilter struct {
	Name           string
	SkippingFilter Filter
}

func NewSkipFilter(skippingFilter Filter) *SkipFilter {
	return &SkipFilter{
		Name:           filterPath + "SkipFilter",
		SkippingFilter: skippingFilter,
	}
}

func (f SkipFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbFilter, err := f.SkippingFilter.ConstructPBFilter()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.SkipFilter{
		Filter: pbFilter,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// TimestampsFilter
type TimestampsFilter struct {
	Name       string
	Timestamps []int64
}

func NewTimestampsFilter(timestamps []int64) *TimestampsFilter {
	return &TimestampsFilter{
		Name:       filterPath + "TimestampsFilter",
		Timestamps: timestamps,
	}
}

func (f TimestampsFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.TimestampsFilter{
		Timestamps: f.Timestamps,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// ValueFilter
type ValueFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

func NewValueFilter(compareFilter CompareFilter) *ValueFilter {
	return &ValueFilter{
		Name:             filterPath + "ValueFilter",
		CompareFilterObj: compareFilter,
	}
}

func (f ValueFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbCompareFilterObj, err := f.CompareFilterObj.ConstructPB()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.ValueFilter{
		CompareFilter: pbCompareFilterObj,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// WhileMatchFilter
type WhileMatchFilter struct {
	Name           string
	MatchingFilter Filter
}

func NewWhileMatchFilter(matchingFilter Filter) *WhileMatchFilter {
	return &WhileMatchFilter{
		Name:           filterPath + "WhileMatchFilter",
		MatchingFilter: matchingFilter,
	}
}

func (f WhileMatchFilter) ConstructPBFilter() (*pb.Filter, error) {
	pbFilter, err := f.MatchingFilter.ConstructPBFilter()
	if err != nil {
		return nil, err
	}
	internalFilter := &pb.WhileMatchFilter{
		Filter: pbFilter,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// FilterAllFilter
type FilterAllFilter struct {
	Name string
}

func NewFilterAllFilter() *FilterAllFilter {
	return &FilterAllFilter{
		Name: filterPath + "FilterAllFilter",
	}
}

func (f FilterAllFilter) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.FilterAllFilter{}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// RowRange
type RowRange struct {
	Name              string
	StartRow          []byte
	StartRowInclusive bool
	StopRow           []byte
	StopRowInclusive  bool
}

func NewRowRange(startRow, stopRow []byte, startRowInclusive, stopRowInclusive bool) *RowRange {
	return &RowRange{
		Name:              filterPath + "RowRange",
		StartRow:          startRow,
		StartRowInclusive: startRowInclusive,
		StopRow:           stopRow,
		StopRowInclusive:  stopRowInclusive,
	}
}

func (f RowRange) ConstructPBFilter() (*pb.Filter, error) {
	internalFilter := &pb.RowRange{
		StartRow:          f.StartRow,
		StartRowInclusive: &f.StartRowInclusive,
		StopRow:           f.StopRow,
		StopRowInclusive:  &f.StopRowInclusive,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}

// MultiRowRangeFilter
type MultiRowRangeFilter struct {
	Name         string
	RowRangeList []*RowRange
}

func NewMultiRowRangeFilter(rowRangeList []*RowRange) *MultiRowRangeFilter {
	return &MultiRowRangeFilter{
		Name:         filterPath + "MultiRowRange",
		RowRangeList: rowRangeList,
	}
}

func (f MultiRowRangeFilter) ConstructPBFilter() (*pb.Filter, error) {
	var pbRowRangeList []*pb.RowRange
	for i := 0; i < len(f.RowRangeList); i++ {
		pbRowRange := &pb.RowRange{
			StartRow:          f.RowRangeList[i].StartRow,
			StartRowInclusive: &f.RowRangeList[i].StartRowInclusive,
			StopRow:           f.RowRangeList[i].StopRow,
			StopRowInclusive:  &f.RowRangeList[i].StopRowInclusive,
		}
		pbRowRangeList = append(pbRowRangeList, pbRowRange)
	}
	internalFilter := &pb.MultiRowRangeFilter{
		RowRangeList: pbRowRangeList,
	}
	serializedFilter, err := proto.Marshal(internalFilter)
	if err != nil {
		return nil, err
	}
	filter := &pb.Filter{
		Name:             &f.Name,
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}
