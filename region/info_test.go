// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/test"
	"github.com/tsuna/gohbase/test/mock"
	"google.golang.org/protobuf/proto"
)

// Test parsing the contents of a cell found in meta.
func TestInfoFromMeta(t *testing.T) {
	put := pb.CellType_PUT
	regionName := []byte("table,,1431921690563.53e41f94d5c3087af0d13259b8c4186d.")
	buf := []byte("PBUF\010\303\217\274\251\326)\022\020\n\007default" +
		"\022\005table\032\000\"\000(\0000\0008\000")
	cell := &hrpc.Cell{
		Row:       regionName,
		Family:    []byte("info"),
		Qualifier: []byte("regioninfo"),
		Timestamp: proto.Uint64(1431921690626),
		CellType:  &put,
	}
	_, err := infoFromCell(cell)
	if err == nil || !strings.HasPrefix(err.Error(), "empty value") {
		t.Errorf("Unexpected error on empty value: %s", err)
	}
	cell.Value = buf
	info, err := infoFromCell(cell)
	if err != nil {
		t.Fatalf("Failed to parse cell: %s", err)
	}
	if !bytes.Equal(info.Name(), regionName) {
		t.Errorf("Unexpected regionName name: %q", info.Name())
	}
	if len(info.StopKey()) != 0 {
		t.Errorf("Expected empty StopKey but got %q", info.StopKey())
	}

	expected := `RegionInfo{Name: "table,,1431921690563.53e41f94d5c3087af0d13259b8c4186d.", ` +
		`ID: 1431921690563, Namespace: "", Table: "table", StartKey: "", StopKey: ""}`
	if s := info.String(); s != expected {
		t.Errorf("Unexpected string representation.\nExpected: %q\n  Actual: %q", expected, s)
	}

	// Set region to be offline
	buf[34] = 0x01
	_, err = infoFromCell(cell)
	if _, ok := err.(OfflineRegionError); !ok {
		t.Fatalf("Unexpected error on offline region: %s", err)
	}

	// Corrupt the protobuf.
	buf[4] = 0xFF
	_, err = infoFromCell(cell)
	if err == nil || !strings.HasPrefix(err.Error(), "failed to decode") {
		t.Errorf("Unexpected error on corrupt protobuf: %s", err)
	}

	// Corrupt the magic number.
	buf[1] = 0xFF
	_, err = infoFromCell(cell)
	if err == nil || !strings.HasPrefix(err.Error(), "invalid magic number") {
		t.Errorf("Unexpected error on invalid magic number %s", err)
	}

	// Corrupt the magic number (first byte).
	buf[0] = 0xFF
	_, err = infoFromCell(cell)
	if err == nil || !strings.HasPrefix(err.Error(), "unsupported region info version") {
		t.Errorf("Unexpected error on invalid magic number %s", err)
	}
}

func TestCompare(t *testing.T) {
	// Test cases from AsyncHBase
	testcases := []struct {
		a, b []byte // Region names, where a > b
	}{{
		// Different table names.
		[]byte("table,,1234567890"), []byte(".META.,,1234567890"),
	}, {
		// Different table names but same prefix.
		[]byte("tabl2,,1234567890"), []byte("tabl1,,1234567890"),
	}, {
		// Different table names (different lengths).
		[]byte("table,,1234567890"), []byte("tabl,,1234567890"),
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

func TestCompareBogusName(t *testing.T) {
	defer func() {
		expected := `no comma found in "bogus" after offset 5`
		v := recover()
		if v == nil {
			t.Errorf("Should have panic'ed")
		} else if e, ok := v.(error); !ok {
			t.Errorf("panic'ed with a %T instead of an error (%#v)", v, v)
		} else if e.Error() != expected {
			t.Errorf("Expected panic(%q) but got %q", expected, e)
		}
	}()
	Compare([]byte("bogus"), []byte("bogus"))
}

func TestRegionInfoMarshalJson(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	var id uint64 = 1
	namespace := []byte("test")
	table := []byte("testTable")
	name := []byte("testTableName")
	startKey := []byte("startKey")
	stopKey := []byte("stopKey")
	queueSize := 30
	flushInterval := 20 * time.Millisecond

	mockConn := mock.NewMockConn(ctrl)

	c := &client{
		conn:          mockConn,
		addr:          "testAddr",
		ctype:         RegionClient,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		inFlight:      20,
		effectiveUser: "effectiveUser",
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
		readTimeout:   DefaultReadTimeout,
	}

	ctx, cancel := context.WithCancel(context.Background())
	info := &info{
		id:        id,
		ctx:       ctx,
		cancel:    cancel,
		namespace: namespace,
		table:     table,
		name:      name,
		startKey:  startKey,
		stopKey:   stopKey,
		specifier: &pb.RegionSpecifier{
			Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte("test"),
		},
		client:    c,
		available: nil,
	}

	jsonVal, err := info.MarshalJSON()

	if err != nil {
		t.Fatalf("Should not have thrown an error: %v", err)
	}

	var jsonUnMarshal map[string]interface{}
	err = json.Unmarshal(jsonVal, &jsonUnMarshal)

	if err != nil {
		t.Fatalf("Error while unmarshalling JSON, %v", err)
	}

	assert.Equal(t, float64(id), jsonUnMarshal["Id"])
	assert.Equal(t, string(table), jsonUnMarshal["Table"])
	assert.Equal(t, string(name), jsonUnMarshal["Name"])
	assert.Equal(t, string(startKey), jsonUnMarshal["StartKey"])
	assert.Equal(t, string(stopKey), jsonUnMarshal["StopKey"])
	assert.Equal(t, true, jsonUnMarshal["Available"])
	assert.Equal(t, string(namespace), jsonUnMarshal["Namespace"])

}

func TestRegionInfoMarshalJsonNilValues(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	info := &info{
		id:        0,
		ctx:       nil,
		cancel:    nil,
		namespace: nil,
		table:     nil,
		name:      nil,
		startKey:  nil,
		stopKey:   nil,
		specifier: nil,
		client:    nil,
		available: nil,
	}

	_, err := info.MarshalJSON()

	if err != nil {
		t.Fatalf("Should not have thrown an error: %v", err)
	}
}
