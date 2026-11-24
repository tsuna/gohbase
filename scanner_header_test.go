// Copyright (C) 2026 The GoHBase Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"testing"
	"time"

	"github.com/tsuna/gohbase/hrpc"
	"google.golang.org/protobuf/proto"
)

type headerCaptureRPCClient struct {
	calls chan hrpc.Call
}

func (c *headerCaptureRPCClient) SendRPC(call hrpc.Call) (proto.Message, error) {
	c.calls <- call
	return nil, nil
}

func newHeaderScanner(t *testing.T) (*scanner, *headerCaptureRPCClient) {
	t.Helper()
	rpc, err := hrpc.NewScan(
		t.Context(),
		[]byte("table"),
		hrpc.HeaderAttribute("trace-id", []byte("trace-123")),
		hrpc.HeaderAttribute("request-class", []byte("interactive")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &headerCaptureRPCClient{calls: make(chan hrpc.Call, 1)}
	return &scanner{
		RPCClient:          client,
		rpc:                rpc,
		curRegionScannerID: 42,
		startRow:           []byte("row"),
	}, client
}

func requireHeaderAttributes(t *testing.T, call hrpc.Call) {
	t.Helper()
	want := []struct {
		name  string
		value string
	}{
		{name: "trace-id", value: "trace-123"},
		{name: "request-class", value: "interactive"},
	}
	attributes := hrpc.GetHeaderAttributes(call)
	if len(attributes) != len(want) {
		t.Fatalf("header attribute count = %d, want %d", len(attributes), len(want))
	}
	for i, attribute := range attributes {
		if got := attribute.GetName(); got != want[i].name {
			t.Errorf("attribute %d name = %q, want %q", i, got, want[i].name)
		}
		if got := string(attribute.GetValue()); got != want[i].value {
			t.Errorf("attribute %d value = %q, want %q", i, got, want[i].value)
		}
	}
}

func TestScannerPreservesHeadersOnContinuation(t *testing.T) {
	s, _ := newHeaderScanner(t)
	call, err := s.makeScanRequest(1)
	if err != nil {
		t.Fatal(err)
	}
	requireHeaderAttributes(t, call)
}

func TestScannerPreservesHeadersOnNewRegion(t *testing.T) {
	s, _ := newHeaderScanner(t)
	s.curRegionScannerID = noScannerID
	call, err := s.makeScanRequest(1)
	if err != nil {
		t.Fatal(err)
	}
	requireHeaderAttributes(t, call)
}

func TestScannerPreservesHeadersOnRenewal(t *testing.T) {
	s, client := newHeaderScanner(t)
	if err := s.renew(t.Context(), 42, []byte("row")); err != nil {
		t.Fatal(err)
	}
	requireHeaderAttributes(t, <-client.calls)
}

func TestScannerPreservesHeadersOnClose(t *testing.T) {
	s, client := newHeaderScanner(t)
	s.closeRegionScanner()
	select {
	case call := <-client.calls:
		requireHeaderAttributes(t, call)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scanner close RPC")
	}
}
