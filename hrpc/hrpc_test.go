// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"github.com/tsuna/gohbase/filter"
	"golang.org/x/net/context"
	"reflect"
	"testing"
)

func TestNewGet(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	key := "45"
	keyb := []byte(key)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	get, err := NewGet(ctx, tableb, keyb)
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, nil, nil) {
		t.Errorf("Get1 didn't set attributes correctly.")
	}
	get, err = NewGetStr(ctx, table, key)
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, nil, nil) {
		t.Errorf("Get2 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Families(fam))
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, fam, nil) {
		t.Errorf("Get3 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1))
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, nil, filter1) {
		t.Errorf("Get4 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1), Families(fam))
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, fam, filter1) {
		t.Errorf("Get5 didn't set attributes correctly.")
	}
}

func confirmGetAttributes(g *Get, ctx context.Context, table, key []byte, fam map[string][]string, filter1 filter.Filter) bool {
	if g.GetContext() != ctx ||
		bytes.Compare(g.Table(), table) != 0 ||
		bytes.Compare(g.Key(), key) != 0 ||
		!reflect.DeepEqual(g.GetFamilies(), fam) ||
		reflect.TypeOf(g.GetFilter()) != reflect.TypeOf(filter1) {
		return false
	}
	return true
}

func TestNewScan(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	start := "0"
	stop := "100"
	startb := []byte("0")
	stopb := []byte("100")
	scan, err := NewScan(ctx, tableb)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, nil, nil, nil, nil) {
		t.Errorf("Scan1 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, startb, stopb, nil, nil) {
		t.Errorf("Scan2 didn't set attributes correctly.")
	}
	scan, err = NewScanStr(ctx, table)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, nil, nil, nil, nil) {
		t.Errorf("Scan3 didn't set attributes correctly.")
	}
	scan, err = NewScanRangeStr(ctx, table, start, stop)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, startb, stopb, nil, nil) {
		t.Errorf("Scan4 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb, Families(fam), Filters(filter1))
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, startb, stopb, fam, filter1) {
		t.Errorf("Scan5 didn't set attributes correctly.")
	}
	scan, err = NewScan(ctx, tableb, Filters(filter1), Families(fam))
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, nil, nil, fam, filter1) {
		t.Errorf("Scan6 didn't set attributes correctly.")
	}
}

func confirmScanAttributes(s *Scan, ctx context.Context, table, start, stop []byte, fam map[string][]string, filter1 filter.Filter) bool {
	if s.GetContext() != ctx ||
		bytes.Compare(s.Table(), table) != 0 ||
		bytes.Compare(s.GetStartRow(), start) != 0 ||
		bytes.Compare(s.GetStopRow(), stop) != 0 ||
		!reflect.DeepEqual(s.GetFamilies(), fam) ||
		reflect.TypeOf(s.GetFilter()) != reflect.TypeOf(filter1) {
		return false
	}
	return true
}
