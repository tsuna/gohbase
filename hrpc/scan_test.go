// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"testing"
)

func TestWithScanStatsHandler(t *testing.T) {
	ctx := context.Background()
	table := []byte("random table")

	s, err := NewScan(ctx, table)
	if err != nil {
		t.Fatal(err)
	}

	if s.ScanStatsHandler() != nil {
		t.Fatal("ScanStatsHandler is not nil as expected")
	}

	h := func(stats *ScanStats) {}

	s, err = NewScan(ctx, table, WithScanStatsHandler(h))
	if err != nil {
		t.Fatal(err)
	}

	if s.ScanStatsHandler() == nil {
		t.Fatal("ScanStatsHandler nil, want it to be set")
	}

	s, err = NewScan(ctx, table, WithScanStatsHandler(nil))
	if err == nil {
		t.Fatal("nil handler should have returned an error")
	}

	_, err = NewGet(ctx, table, []byte("random key"), WithScanStatsHandler(h))
	if err == nil {
		t.Fatal("Should not have been able to create non-Scan request without error")
	}
}

func TestScanStatsID(t *testing.T) {
	var (
		ctx        = context.Background()
		table      = []byte("random table")
		expectedID = int64(11)
	)

	s, err := NewScan(ctx, table, ScanStatsID(expectedID))
	if err != nil {
		t.Fatal(err)
	}
	if s.ScanStatsID() != expectedID {
		t.Fatalf("ScanStatsID = %d, expected %d", s.ScanStatsID(), expectedID)
	}

	s, err = NewScan(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	if s.ScanStatsID() == expectedID {
		t.Fatalf("ScanStatsID on a new Scan request should be reset to a random num")
	}

	_, err = NewGet(ctx, table, []byte("random key"), ScanStatsID(expectedID))
	if err == nil {
		t.Fatal("Should not have been able to create non-Scan request without error")
	}
}
