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

// FilterListOperator is TODO
type FilterListOperator int32

func (o FilterListOperator) isValid() bool {
	return (o >= 1 && o <= 2)
}

// Constants is TODO
const (
	MustPassAll FilterListOperator = 1
	MustPassOne FilterListOperator = 2
)

// CompareType is TODO
type CompareType int32

func (c CompareType) isValid() bool {
	return (c >= 0 && c <= 6)
}

// Constants is TODO
const (
	Less           CompareType = 0
	LessOrEqual    CompareType = 1
	Equal          CompareType = 2
	NotEqual       CompareType = 3
	GreaterOrEqual CompareType = 4
	Greater        CompareType = 5
	NoOp           CompareType = 6
)

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

// Filter is TODO
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

// BytesBytesPair is a type used in FuzzyRowFilter. Want to avoid users having
// to interact directly with the protobuf generated file so exposing here.
type BytesBytesPair struct {
	First  []byte
	Second []byte
}

// NewBytesBytesPair is TODO
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

// FilterList is TODO
type FilterList struct {
	Name     string
	Operator FilterListOperator
	Filters  []Filter
}

// NewFilterList is TODO
func NewFilterList(operator FilterListOperator, filters ...Filter) *FilterList {
	return &FilterList{
		Name:     filterPath + "FilterList",
		Operator: operator,
		Filters:  filters,
	}
}

// AddFilters is TODO
func (f *FilterList) AddFilters(filters ...Filter) {
	f.Filters = append(f.Filters, filters...)
}

// ConstructPBFilter is TODO
func (f *FilterList) ConstructPBFilter() (*pb.Filter, error) {
	if !f.Operator.isValid() {
		return nil, errors.New("Invalid operator specified.")
	}
	p := pb.FilterList_Operator(f.Operator)
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

// ColumnCountGetFilter is TODO
type ColumnCountGetFilter struct {
	Name  string
	Limit int32
}

// NewColumnCountGetFilter is TODO
func NewColumnCountGetFilter(limit int32) *ColumnCountGetFilter {
	return &ColumnCountGetFilter{
		Name:  filterPath + "ColumnCountGetFilter",
		Limit: limit,
	}
}

// ConstructPBFilter is TODO
func (f *ColumnCountGetFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// ColumnPaginationFilter is TODO
type ColumnPaginationFilter struct {
	Name         string
	Limit        int32
	Offset       int32
	ColumnOffset []byte
}

// NewColumnPaginationFilter is TODO
func NewColumnPaginationFilter(limit, offset int32, columnOffset []byte) *ColumnPaginationFilter {
	return &ColumnPaginationFilter{
		Name:         filterPath + "ColumnPaginationFilter",
		Limit:        limit,
		Offset:       offset,
		ColumnOffset: columnOffset,
	}
}

// ConstructPBFilter is TODO
func (f *ColumnPaginationFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// ColumnPrefixFilter is TODO
type ColumnPrefixFilter struct {
	Name   string
	Prefix []byte
}

// NewColumnPrefixFilter is TODO
func NewColumnPrefixFilter(prefix []byte) *ColumnPrefixFilter {
	return &ColumnPrefixFilter{
		Name:   filterPath + "ColumnPrefixFilter",
		Prefix: prefix,
	}
}

// ConstructPBFilter is TODO
func (f *ColumnPrefixFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// ColumnRangeFilter is TODO
type ColumnRangeFilter struct {
	Name               string
	MinColumn          []byte
	MinColumnInclusive bool
	MaxColumn          []byte
	MaxColumnInclusive bool
}

// NewColumnRangeFilter is TODO
func NewColumnRangeFilter(minColumn, maxColumn []byte,
	minColumnInclusive, maxColumnInclusive bool) *ColumnRangeFilter {
	return &ColumnRangeFilter{
		Name:               filterPath + "ColumnRangeFilter",
		MinColumn:          minColumn,
		MaxColumn:          maxColumn,
		MinColumnInclusive: minColumnInclusive,
		MaxColumnInclusive: maxColumnInclusive,
	}
}

// ConstructPBFilter is TODO
func (f *ColumnRangeFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// CompareFilter is TODO
type CompareFilter struct {
	Name          string
	CompareOp     CompareType
	ComparatorObj Comparator
}

// NewCompareFilter is TODO
func NewCompareFilter(compareOp CompareType, comparatorObj Comparator) *CompareFilter {
	return &CompareFilter{
		Name:          filterPath + "CompareFilter",
		CompareOp:     compareOp,
		ComparatorObj: comparatorObj,
	}
}

// ConstructPB is TODO
func (f *CompareFilter) ConstructPB() (*pb.CompareFilter, error) {
	if !f.CompareOp.isValid() {
		return nil, errors.New("Invalid compare operation specified.")
	}
	p := pb.CompareType(f.CompareOp)
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

// ConstructPBFilter is TODO
func (f *CompareFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// DependentColumnFilter is TODO
type DependentColumnFilter struct {
	Name                string
	CompareFilterObj    CompareFilter
	ColumnFamily        []byte
	ColumnQualifier     []byte
	DropDependentColumn bool
}

// NewDependentColumnFilter is TODO
func NewDependentColumnFilter(compareFilter CompareFilter, columnFamily, columnQualifier []byte,
	dropDependentColumn bool) *DependentColumnFilter {
	return &DependentColumnFilter{
		Name:                filterPath + "DependentColumnFilter",
		CompareFilterObj:    compareFilter,
		ColumnFamily:        columnFamily,
		ColumnQualifier:     columnQualifier,
		DropDependentColumn: dropDependentColumn,
	}
}

// ConstructPBFilter is TODO
func (f *DependentColumnFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// FamilyFilter is TODO
type FamilyFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

// NewFamilyFilter is TODO
func NewFamilyFilter(compareFilter CompareFilter) *FamilyFilter {
	return &FamilyFilter{
		Name:             filterPath + "FamilyFilter",
		CompareFilterObj: compareFilter,
	}
}

// ConstructPBFilter is TODO
func (f *FamilyFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// FilterWrapper is TODO
type FilterWrapper struct {
	Name          string
	WrappedFilter Filter
}

// NewFilterWrapper is TODO
func NewFilterWrapper(wrappedFilter Filter) *FilterWrapper {
	return &FilterWrapper{
		Name:          filterPath + "FilterWrapper",
		WrappedFilter: wrappedFilter,
	}
}

// ConstructPBFilter is TODO
func (f *FilterWrapper) ConstructPBFilter() (*pb.Filter, error) {
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

// FirstKeyOnlyFilter is TODO
type FirstKeyOnlyFilter struct {
	Name string
}

// NewFirstKeyOnlyFilter is TODO
func NewFirstKeyOnlyFilter() *FirstKeyOnlyFilter {
	return &FirstKeyOnlyFilter{
		Name: filterPath + "FirstKeyOnlyFilter",
	}
}

// ConstructPBFilter is TODO
func (f *FirstKeyOnlyFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// FirstKeyValueMatchingQualifiersFilter is TODO
type FirstKeyValueMatchingQualifiersFilter struct {
	Name       string
	Qualifiers [][]byte
}

// NewFirstKeyValueMatchingQualifiersFilter is TODO
func NewFirstKeyValueMatchingQualifiersFilter(qualifiers [][]byte) *FirstKeyValueMatchingQualifiersFilter {
	return &FirstKeyValueMatchingQualifiersFilter{
		Name:       filterPath + "FirstKeyValueMatchingQualifiersFilter",
		Qualifiers: qualifiers,
	}
}

// ConstructPBFilter is TODO
func (f *FirstKeyValueMatchingQualifiersFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// FuzzyRowFilter is TODO
type FuzzyRowFilter struct {
	Name          string
	FuzzyKeysData []*BytesBytesPair
}

// NewFuzzyRowFilter is TODO
func NewFuzzyRowFilter(pairs []*BytesBytesPair) *FuzzyRowFilter {
	return &FuzzyRowFilter{
		Name:          filterPath + "FuzzyRowFilter",
		FuzzyKeysData: pairs,
	}
}

// ConstructPBFilter is TODO
func (f *FuzzyRowFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// InclusiveStopFilter is TODO
type InclusiveStopFilter struct {
	Name       string
	StopRowKey []byte
}

// NewInclusiveStopFilter is TODO
func NewInclusiveStopFilter(stopRowKey []byte) *InclusiveStopFilter {
	return &InclusiveStopFilter{
		Name:       filterPath + "InclusiveStopFilter",
		StopRowKey: stopRowKey,
	}
}

// ConstructPBFilter is TODO
func (f *InclusiveStopFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// KeyOnlyFilter is TODO
type KeyOnlyFilter struct {
	Name     string
	LenAsVal bool
}

// NewKeyOnlyFilter is TODO
func NewKeyOnlyFilter(lenAsVal bool) *KeyOnlyFilter {
	return &KeyOnlyFilter{
		Name:     filterPath + "KeyOnlyFilter",
		LenAsVal: lenAsVal,
	}
}

// ConstructPBFilter is TODO
func (f *KeyOnlyFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// MultipleColumnPrefixFilter is TODO
type MultipleColumnPrefixFilter struct {
	Name           string
	SortedPrefixes [][]byte
}

// NewMultipleColumnPrefixFilter is TODO
func NewMultipleColumnPrefixFilter(sortedPrefixes [][]byte) *MultipleColumnPrefixFilter {
	return &MultipleColumnPrefixFilter{
		Name:           filterPath + "MultipleColumnPrefixFilter",
		SortedPrefixes: sortedPrefixes,
	}
}

// ConstructPBFilter is TODO
func (f *MultipleColumnPrefixFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// PageFilter is TODO
type PageFilter struct {
	Name     string
	PageSize int64
}

// NewPageFilter is TODO
func NewPageFilter(pageSize int64) *PageFilter {
	return &PageFilter{
		Name:     filterPath + "PageFilter",
		PageSize: pageSize,
	}
}

// ConstructPBFilter is TODO
func (f *PageFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// PrefixFilter is TODO
type PrefixFilter struct {
	Name   string
	Prefix []byte
}

// NewPrefixFilter is TODO
func NewPrefixFilter(prefix []byte) *PrefixFilter {
	return &PrefixFilter{
		Name:   filterPath + "PrefixFilter",
		Prefix: prefix,
	}
}

// ConstructPBFilter is TODO
func (f *PrefixFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// QualifierFilter is TODO
type QualifierFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

// NewQualifierFilter is TODO
func NewQualifierFilter(compareFilter CompareFilter) *QualifierFilter {
	return &QualifierFilter{
		Name:             filterPath + "QualifierFilter",
		CompareFilterObj: compareFilter,
	}
}

// ConstructPBFilter is TODO
func (f *QualifierFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// RandomRowFilter is TODO
type RandomRowFilter struct {
	Name   string
	Chance float32
}

// NewRandomRowFilter is TODO
func NewRandomRowFilter(chance float32) *RandomRowFilter {
	return &RandomRowFilter{
		Name:   filterPath + "RandomRowFilter",
		Chance: chance,
	}
}

// ConstructPBFilter is TODO
func (f *RandomRowFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// RowFilter is TODO
type RowFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

// NewRowFilter is TODO
func NewRowFilter(compareFilter CompareFilter) *RowFilter {
	return &RowFilter{
		Name:             filterPath + "RowFilter",
		CompareFilterObj: compareFilter,
	}
}

// ConstructPBFilter is TODO
func (f *RowFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// SingleColumnValueFilter is TODO
type SingleColumnValueFilter struct {
	Name              string
	ColumnFamily      []byte
	ColumnQualifier   []byte
	CompareOp         CompareType
	ComparatorObj     Comparator
	FilterIfMissing   bool
	LatestVersionOnly bool
}

// NewSingleColumnValueFilter is TODO
func NewSingleColumnValueFilter(columnFamily, columnQualifier []byte, compareOp CompareType,
	comparatorObj Comparator, filterIfMissing, latestVersionOnly bool) *SingleColumnValueFilter {
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

// ConstructPB is TODO
func (f *SingleColumnValueFilter) ConstructPB() (*pb.SingleColumnValueFilter, error) {
	if !f.CompareOp.isValid() {
		return nil, errors.New("Invalid compare operation specified.")
	}
	p := pb.CompareType(f.CompareOp)
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

// ConstructPBFilter is TODO
func (f *SingleColumnValueFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// SingleColumnValueExcludeFilter is TODO
type SingleColumnValueExcludeFilter struct {
	Name                       string
	SingleColumnValueFilterObj SingleColumnValueFilter
}

// NewSingleColumnValueExcludeFilter is TODO
func NewSingleColumnValueExcludeFilter(filter SingleColumnValueFilter) *SingleColumnValueExcludeFilter {
	return &SingleColumnValueExcludeFilter{
		Name: filterPath + "SingleColumnValueExcludeFilter",
		SingleColumnValueFilterObj: filter,
	}
}

// ConstructPBFilter is TODO
func (f *SingleColumnValueExcludeFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// SkipFilter is TODO
type SkipFilter struct {
	Name           string
	SkippingFilter Filter
}

// NewSkipFilter is TODO
func NewSkipFilter(skippingFilter Filter) *SkipFilter {
	return &SkipFilter{
		Name:           filterPath + "SkipFilter",
		SkippingFilter: skippingFilter,
	}
}

// ConstructPBFilter is TODO
func (f *SkipFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// TimestampsFilter is TODO
type TimestampsFilter struct {
	Name       string
	Timestamps []int64
}

// NewTimestampsFilter is TODO
func NewTimestampsFilter(timestamps []int64) *TimestampsFilter {
	return &TimestampsFilter{
		Name:       filterPath + "TimestampsFilter",
		Timestamps: timestamps,
	}
}

// ConstructPBFilter is TODO
func (f *TimestampsFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// ValueFilter is TODO
type ValueFilter struct {
	Name             string
	CompareFilterObj CompareFilter
}

// NewValueFilter is TODO
func NewValueFilter(compareFilter CompareFilter) *ValueFilter {
	return &ValueFilter{
		Name:             filterPath + "ValueFilter",
		CompareFilterObj: compareFilter,
	}
}

// ConstructPBFilter is TODO
func (f *ValueFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// WhileMatchFilter is TODO
type WhileMatchFilter struct {
	Name           string
	MatchingFilter Filter
}

// NewWhileMatchFilter is TODO
func NewWhileMatchFilter(matchingFilter Filter) *WhileMatchFilter {
	return &WhileMatchFilter{
		Name:           filterPath + "WhileMatchFilter",
		MatchingFilter: matchingFilter,
	}
}

// ConstructPBFilter is TODO
func (f *WhileMatchFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// FilterAllFilter is TODO
type FilterAllFilter struct {
	Name string
}

// NewFilterAllFilter is TODO
func NewFilterAllFilter() *FilterAllFilter {
	return &FilterAllFilter{
		Name: filterPath + "FilterAllFilter",
	}
}

// ConstructPBFilter is TODO
func (f *FilterAllFilter) ConstructPBFilter() (*pb.Filter, error) {
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

// RowRange is TODO
type RowRange struct {
	Name              string
	StartRow          []byte
	StartRowInclusive bool
	StopRow           []byte
	StopRowInclusive  bool
}

// NewRowRange is TODO
func NewRowRange(startRow, stopRow []byte, startRowInclusive, stopRowInclusive bool) *RowRange {
	return &RowRange{
		Name:              filterPath + "RowRange",
		StartRow:          startRow,
		StartRowInclusive: startRowInclusive,
		StopRow:           stopRow,
		StopRowInclusive:  stopRowInclusive,
	}
}

// ConstructPBFilter is TODO
func (f *RowRange) ConstructPBFilter() (*pb.Filter, error) {
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

// MultiRowRangeFilter is TODO
type MultiRowRangeFilter struct {
	Name         string
	RowRangeList []*RowRange
}

// NewMultiRowRangeFilter is TODO
func NewMultiRowRangeFilter(rowRangeList []*RowRange) *MultiRowRangeFilter {
	return &MultiRowRangeFilter{
		Name:         filterPath + "MultiRowRange",
		RowRangeList: rowRangeList,
	}
}

// ConstructPBFilter is TODO
func (f *MultiRowRangeFilter) ConstructPBFilter() (*pb.Filter, error) {
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
