// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region_test

import (
	"testing"

	. "github.com/tsuna/gohbase/region"
)

func TestCompare(t *testing.T) {
	// Test cases from AsyncHBase
	testcases := []struct {
		a, b []byte // Region names, where a > b
	}{{
		// Different table names.
		[]byte("table,,1234567890"), []byte(".META.,,1234567890"),
	}, {
		// Any key is greater than the start key.
		[]byte("table,foo,1234567890"), []byte("table,,1234567890"),
	}, {
		// Different keys.
		[]byte("table,foo,1234567890"), []byte("table,bar,1234567890"),
	}, {
		// Shorter key is smaller than longer key.
		[]byte("table,fool,1234567890"), []byte("table,foo,1234567890"),
	}, {
		// Properly handle keys that contain commas.
		[]byte("table,a,,c,1234567890"), []byte("table,a,,b,1234567890"),
	}, {
		// If keys are equal, then start code should break the tie.
		[]byte("table,foo,1234567891"), []byte("table,foo,1234567890"),
	}, {
		// Make sure that a start code being a prefix of another is handled.
		[]byte("table,foo,1234567890"), []byte("table,foo,123456789"),
	}, {
		// If both are start keys, then start code should break the tie.
		[]byte("table,,1234567891"), []byte("table,,1234567890"),
	}, {
		// The value `:' is always greater than any start code.
		[]byte("table,foo,:"), []byte("table,foo,9999999999"),
	}, {
		// Issue 27: searching for key "8,\001" and region key is "8".
		[]byte("table,8,\001,:"), []byte("table,8,1339667458224"),
	}}

	for _, tcase := range testcases {
		if i := Compare(tcase.a, tcase.b); i <= 0 {
			t.Errorf("%q was found to be less than %q (%d)", tcase.a, tcase.b, i)
		}
		if i := Compare(tcase.b, tcase.a); i >= 0 {
			t.Errorf("%q was found to be greater than %q (%d)", tcase.b, tcase.a, i)
		}
	}
}
