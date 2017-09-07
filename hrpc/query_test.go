// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aristanetworks/goarista/test"
	"github.com/tsuna/gohbase/filter"
)

func TestFamilesOption(t *testing.T) {
	f := map[string][]string{"yolo": []string{"swag", "meow"}}

	g, err := NewGet(context.Background(), nil, nil, Families(f))
	if err != nil {
		t.Error(err)
	}

	if d := test.Diff(f, g.families); d != "" {
		t.Error(d)
	}

	_, err = NewPutStr(context.Background(), "", "", nil, Families(f))
	if err == nil || err.Error() != "'Families' option can only be used with Get or Scan request" {
		t.Error(err)
	}
}

func TestFiltersOption(t *testing.T) {
	f := filter.NewColumnCountGetFilter(1)
	g, err := NewGet(context.Background(), nil, nil, Filters(f))
	if err != nil {
		t.Error(err)
	}

	if d := test.Diff(f, g.filter); d != "" {
		t.Error(d)
	}

	_, err = NewPutStr(context.Background(), "", "", nil, Filters(f))
	if err == nil || err.Error() != "'Filters' option can only be used with Get or Scan request" {
		t.Error(err)
	}
}

func TestTimeRangeOption(t *testing.T) {
	now := time.Now()
	tests := []struct {
		from time.Time
		to   time.Time
		err  error
	}{
		{from: now, to: now.Add(time.Minute)},
		{from: now.Add(time.Minute), to: now, err: errors.New(
			"'from' timestamp is greater or equal to 'to' timestamp")},
		{from: now, to: now, err: errors.New(
			"'from' timestamp is greater or equal to 'to' timestamp")},
	}

	for _, tcase := range tests {
		g, err := NewGet(context.Background(), nil, nil, TimeRange(tcase.from, tcase.to))
		if d := test.Diff(tcase.err, err); d != "" {
			t.Errorf(d)
		}

		if tcase.err != nil {
			continue
		}

		from, to := g.fromTimestamp, g.toTimestamp
		if fromExp := uint64(tcase.from.UnixNano() / 1e6); from != fromExp {
			t.Errorf("expected from time %d, got from time %d", fromExp, from)
		}
		if toExp := uint64(tcase.to.UnixNano() / 1e6); to != toExp {
			t.Errorf("expected to time %d, got to time %d", toExp, to)
		}

		_, err = NewPutStr(context.Background(), "", "", nil, TimeRange(tcase.to, tcase.from))
		if err == nil || err.Error() !=
			"'TimeRange' option can only be used with Get or Scan request" {
			t.Error(err)
		}
	}
}

func TestMaxVersions(t *testing.T) {
	v := uint32(123456)
	g, err := NewGet(context.Background(), nil, nil, MaxVersions(v))
	if err != nil {
		t.Error(err)
	}

	if vExp, vGot := v, g.maxVersions; vExp != vGot {
		t.Errorf("expected %d, got %d", vExp, vGot)
	}

	g, err = NewGet(context.Background(), nil, nil, MaxVersions(uint32(math.MaxUint32)))
	if err == nil || err.Error() != "'MaxVersions' exceeds supported number of versions" {
		t.Error(err)
	}

	_, err = NewPutStr(context.Background(), "", "", nil, MaxVersions(v))
	if err == nil || err.Error() !=
		"'MaxVersions' option can only be used with Get or Scan request" {
		t.Error(err)
	}
}

func TestMaxResultsPerColumnFamily(t *testing.T) {
	r := uint32(123456)
	g, err := NewGet(context.Background(), nil, nil, MaxResultsPerColumnFamily(r))
	if err != nil {
		t.Error(err)
	}

	if rExp, rGot := r, g.storeLimit; rExp != rGot {
		t.Errorf("expected %d, got %d", rExp, rGot)
	}

	g, err = NewGet(context.Background(), nil, nil,
		MaxResultsPerColumnFamily(uint32(math.MaxUint32)))
	if err == nil || err.Error() !=
		"'MaxResultsPerColumnFamily' exceeds supported number of value results" {
		t.Error(err)
	}

	_, err = NewPutStr(context.Background(), "", "", nil, MaxResultsPerColumnFamily(r))
	if err == nil || err.Error() !=
		"'MaxResultsPerColumnFamily' option can only be used with Get or Scan request" {
		t.Error(err)
	}
}

func TestResultOffset(t *testing.T) {
	r := uint32(123456)
	g, err := NewGet(context.Background(), nil, nil, ResultOffset(r))
	if err != nil {
		t.Error(err)
	}

	if rExp, rGot := r, g.storeOffset; rExp != rGot {
		t.Errorf("expected %d, got %d", rExp, rGot)
	}

	g, err = NewGet(context.Background(), nil, nil, ResultOffset(uint32(math.MaxUint32)))
	if err == nil || err.Error() != "'ResultOffset' exceeds supported offset value" {
		t.Error(err)
	}

	_, err = NewPutStr(context.Background(), "", "", nil, ResultOffset(r))
	if err == nil || err.Error() !=
		"'ResultOffset' option can only be used with Get or Scan request" {
		t.Error(err)
	}
}
