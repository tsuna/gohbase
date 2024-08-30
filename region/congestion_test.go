// Copyright (C) 2024  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"errors"
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsuna/gohbase/hrpc"
	"google.golang.org/protobuf/proto"
)

type resp struct {
	rpc hrpc.Call
	err error
}

type fakeRegionServer struct {
	reqs  chan hrpc.Call
	resps chan resp
}

func (rs *fakeRegionServer) trySend(rpc hrpc.Call) error {
	rs.reqs <- rpc
	return nil
}

func (rs *fakeRegionServer) receive(io.Reader) (hrpc.Call, proto.Message, error) {
	resp := <-rs.resps
	return resp.rpc, nil, resp.err
}

type fakeGauge struct {
	prometheus.Gauge
}

func (fakeGauge) Set(float64) {}

func newRPC() hrpc.Call {
	get, err := hrpc.NewGet(nil, nil, nil)
	if err != nil {
		panic(err)
	}
	return get
}

func TestCongestion(t *testing.T) {
	rs := fakeRegionServer{
		reqs:  make(chan hrpc.Call, 1),
		resps: make(chan resp, 1),
	}
	cc := newCongestion(nil, 1, 10, fakeGauge{})
	cc.trySend = rs.trySend
	cc.receive = rs.receive

	if cc.sendWindow != 5 || cc.sema.v != 5 {
		t.Fatalf("unexpected starting values: sendWindow: %d sema.v: %d", cc.sendWindow, cc.sema.v)
	}

	var rpcs []hrpc.Call
	for i := range 5 {
		rpc := newRPC()
		if err := cc.send(rpc); err != nil {
			t.Fatalf("unexpected error from send: %s", err)
		}
		if cc.sema.v != 5-(i+1) {
			t.Errorf("expected sema to be at %d, but got: %d", 5-(i+1), cc.sema.v)
		}
		rpcs = append(rpcs, <-rs.reqs)
	}

	// The next send should block, so do it in a goroutine
	go func() {
		rpc := newRPC()
		if err := cc.send(rpc); err != nil {
			t.Errorf("unexpected error from send: %s", err)
		}
	}()

	select {
	case rpc := <-rs.reqs:
		t.Errorf("send should have been blocked, but an rpc was sent: %v", rpc)
	default:
	}

	// receive a response, which should unblock our goroutine above
	rs.resps <- resp{rpcs[0], nil}
	if err := cc.read(nil); err != nil {
		t.Errorf("unexpected error from receive: %s", err)
	}
	if res := <-rpcs[0].ResultChan(); res.Error != nil {
		t.Errorf("unexpected error on response: %s", res.Error)
	}
	if cc.sendWindow != 6 {
		t.Errorf("expected sendWindow to expand to 6, got %d", cc.sendWindow)
	}
	rpcs = rpcs[1:]
	// accept send from above goroutine
	rpcs = append(rpcs, <-rs.reqs)
	// sema was at 0, then we received a successful response, which
	// should have added 2, and then sent another request so it should
	// be 1 now.
	if cc.sema.v != 1 {
		t.Errorf("expected sema to be at 1, but it's %d", cc.sema.v)
	}
	for i, rpc := range rpcs {
		rs.resps <- resp{rpc, nil}
		if err := cc.read(nil); err != nil {
			t.Errorf("unexpected error from receive: %s", err)
		}
		if res := <-rpc.ResultChan(); res.Error != nil {
			t.Errorf("unexpected error on response: %s", res.Error)
		}
		// sendWindow should be increasing by one on each successful receive
		if cc.sendWindow != min(6+(i+1), 10) {
			t.Errorf("expected sendWindow to be %d, got %d", min(6+i, 10), cc.sendWindow)
		}
		// sema.v should be increasing by two on each successful receive
		if cc.sema.v != min(1+(i+1)*2, 10) {
			t.Errorf("expected sema.v to be %d, got %d", min(1+(i+1)*2, 10), cc.sema.v)
		}
	}

	rpcs = rpcs[:0]

	// Send 10 requests to fill the send window and then fail them all
	for i := range 10 {
		rpc := newRPC()
		if err := cc.send(rpc); err != nil {
			t.Fatalf("unexpected error from send: %s", err)
		}
		if cc.sema.v != 10-(i+1) {
			t.Errorf("expected sema to be at %d, but got: %d", 5-(i+1), cc.sema.v)
		}
		rpcs = append(rpcs, <-rs.reqs)
	}
	// sendWindow should halve on each failure
	expectedSendWindow := []int{5, 2, 1, 1, 1, 1, 1, 1, 1, 1}
	// sema will be decreasing by the change in sendWindow, but
	// also increasing by 1 for each rpc read.
	expectedSema := []int{-4, -6, -6, -5, -4, -3, -2, -1, 0, 1}
	for i, rpc := range rpcs {
		rs.resps <- resp{rpc, RetryableError{}}
		if err := cc.read(nil); err != nil {
			t.Errorf("unexpected error from recieve: %s", err)
		}
		if cc.sendWindow != expectedSendWindow[i] {
			t.Errorf("expected sendWindow to be %d, got %d", expectedSendWindow[i], cc.sendWindow)
		}
		if cc.sema.v != expectedSema[i] {
			t.Errorf("expected sema.v to be %d, got %d", expectedSema[i], cc.sema.v)
		}
	}

	// Send one more RPC and fail it
	if err := cc.send(newRPC()); err != nil {
		t.Fatalf("unexpected error from send: %s", err)
	}
	rpc := <-rs.reqs
	rs.resps <- resp{rpc, RetryableError{}}
	if err := cc.read(nil); err != nil {
		t.Errorf("unexpected error from recieve: %s", err)
	}
	// read should not have blocked sending to the retry channel and
	// instead returned the error to the caller:
	if res := <-rpc.ResultChan(); !errors.Is(res.Error, RetryableError{}) {
		t.Errorf("expected to see RetryAble error result on rpc, got: %v", res)
	}

	// All the failed rpc's should be on the retry channel
	for i, rpc := range rpcs {
		if got := <-cc.retry; got != rpc {
			t.Errorf("unexpected rpc %d on retry chan, exp: %p got: %p", i, rpc, got)
		}
	}
}

// func TestRetries(t *testing.T) {
// 	// create connection that fails every other request. Send 200
// 	// requests with maxWindowSize 100 and verify they all make it
// 	// through.
// }
