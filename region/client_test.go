// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aristanetworks/goarista/test"
	"github.com/golang/mock/gomock"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/test/mock"
	"golang.org/x/net/context"
)

var ctrl *gomock.Controller

func TestErrors(t *testing.T) {
	ue := UnrecoverableError{fmt.Errorf("oops")}
	if ue.Error() != "oops" {
		t.Errorf("Wrong error message. Got %q, wanted %q", ue, "oops")
	}
}

func TestWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockReadWriteCloser(ctrl)
	c := &client{
		conn: mockConn,
	}

	// check if Write returns an error
	expectErr := errors.New("nope")
	mockConn.EXPECT().Write(gomock.Any()).Return(0, expectErr).Times(1)
	err := c.write([]byte("lol"))
	if diff := test.Diff(expectErr, err); diff != "" {
		t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
			expectErr, err, diff)
	}

	// check if it returns ErrShortWrite
	mockConn.EXPECT().Write(gomock.Any()).Return(1, nil).Times(1)
	err = c.write([]byte("lol"))
	if diff := test.Diff(ErrShortWrite, err); diff != "" {
		t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
			ErrShortWrite, err, diff)
	}

	// check if it actually writes the right data
	expected := []byte("lol")
	mockConn.EXPECT().Write(gomock.Any()).Return(3, nil).Times(1).Do(func(buf []byte) {
		if diff := test.Diff(expected, buf); diff != "" {
			t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
				expected, buf, diff)
		}
	})
	err = c.write(expected)
	if err != nil {
		t.Errorf("Was expecting error, but got one: %#v", err)
	}
}

func TestSendHello(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockReadWriteCloser(ctrl)
	c := &client{
		conn: mockConn,
	}

	// check if it's sending the right "hello" for RegionClient
	mockConn.EXPECT().Write(gomock.Any()).Return(35, nil).Times(1).Do(func(buf []byte) {
		expected := []byte("HBas\x00P\x00\x00\x00\x19\n\b\n\x06gopher\x12\rClientService")
		if diff := test.Diff(expected, buf); diff != "" {
			t.Errorf("Type RegionClient:\n Expected: %#v\nReceived: %#v\nDiff:%s",
				expected, buf, diff)
		}
	})
	err := c.sendHello(RegionClient)
	if err != nil {
		t.Errorf("Was expecting error, but got one: %#v", err)
	}

	// check if it sends the right "hello" for MasterClient
	mockConn.EXPECT().Write(gomock.Any()).Return(35, nil).Times(1).Do(func(buf []byte) {
		expected := []byte("HBas\x00P\x00\x00\x00\x19\n\b\n\x06gopher\x12\rMasterService")
		if diff := test.Diff(expected, buf); diff != "" {
			t.Errorf("Type MasterClient:\n Expected: %#v\nReceived: %#v\nDiff:%s",
				expected, buf, diff)
		}
	})
	err = c.sendHello(MasterClient)
	if err != nil {
		t.Errorf("Was expecting error, but got one: %#v", err)
	}
}

func TestFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockReadWriteCloser(ctrl)
	c := &client{
		conn: mockConn,
		done: make(chan struct{}),
		rpcs: make(chan hrpc.Call),
		sent: make(map[uint32]hrpc.Call),
	}
	expectedErr := errors.New("oooups")

	//  populate sent map, and test if it sends response err for everything
	var i uint32
	var wgResults sync.WaitGroup
	for i = 0; i < 100; i++ {
		mockCall := mock.NewMockCall(ctrl)
		ch := make(chan hrpc.RPCResult)
		mockCall.EXPECT().ResultChan().Return(ch).Times(1)
		c.sent[i] = mockCall
		wgResults.Add(1)
		go func() {
			select {
			case <-time.After(2 * time.Second):
				t.Errorf("result hasn't been received")
			case r := <-ch:
				if diff := test.Diff(expectedErr, r.Error); diff != "" {
					t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
						expectedErr, r.Error, diff)
				}
			}
			wgResults.Done()
		}()
	}

	// check that connection Close is called only once
	mockConn.EXPECT().Close().Times(1)

	// run multiple in parallel to make sure everything is called only once
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			c.fail(expectedErr)
			wg.Done()
		}()
	}
	wg.Wait()

	wgResults.Wait()
	// check if map is empty
	if len(c.sent) != 0 {
		t.Errorf("Sent map length is %d, expected 0", len(c.sent))
	}

	// check if done channel is closed to notify goroutines to stop
	// if close(c.done) is called more than once, it would panic
	select {
	case <-time.After(2 * time.Second):
		t.Errorf("done hasn't been closed")
	case _, more := <-c.done:
		if more {
			t.Error("expected done to be closed")
		}
	}

	// check if rpcs channel is closed
	// if close(c.rpcs) is called more than once, it would panic
	select {
	case <-time.After(2 * time.Second):
		t.Errorf("rpcs hasn't been closed")
	case _, more := <-c.rpcs:
		if more {
			t.Error("expected rpcs to be closed")
		}
	}

	if diff := test.Diff(expectedErr, c.err); diff != "" {
		t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
			expectedErr, c.err, diff)
	}
}

type rpcMatcher struct {
	payload string
}

func (m rpcMatcher) Matches(x interface{}) bool {
	data, ok := x.([]byte)
	if !ok {
		return false
	}
	return strings.HasSuffix(string(data), m.payload)
}

func (m rpcMatcher) String() string {
	return "RPC payload is equal to " + m.payload
}

func newRPCMatcher(payload string) gomock.Matcher {
	return rpcMatcher{payload: payload}
}

func TestQueueRPC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queueSize := 30
	flushInterval := 20 * time.Millisecond
	mockConn := mock.NewMockReadWriteCloser(ctrl)
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call, queueSize),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
	}
	go c.processRPCs() // Writer goroutine

	// define rpcs behaviour
	var wgWrites sync.WaitGroup
	calls := make([]hrpc.Call, 100)
	ctx := context.Background()
	for i := range calls {
		wgWrites.Add(1)
		mockCall := mock.NewMockCall(ctrl)
		mockCall.EXPECT().Name().Return("lol").Times(1)
		payload := fmt.Sprintf("rpc_%d", i)
		mockCall.EXPECT().Serialize().Return([]byte(payload), nil).Times(1)
		mockCall.EXPECT().Context().Return(ctx).Times(1)
		mockCall.EXPECT().ResultChan().Return(make(chan hrpc.RPCResult, 1)).Times(1)
		calls[i] = mockCall

		// we expect that it eventually writes to connection
		mockConn.EXPECT().Write(newRPCMatcher(payload)).Times(1).Return(15+len(payload), nil).Do(
			func(buf []byte) {
				wgWrites.Done()
			})
	}

	// queue calls in parallel
	for _, call := range calls {
		go func(call hrpc.Call) {
			c.QueueRPC(call)
		}(call)
	}

	// wait till all calls complete
	done := make(chan struct{})
	go func() {
		wgWrites.Wait()
		close(done)
	}()
	select {
	case <-time.After(2 * time.Second):
		t.Fatalf("rpcs hasn't been written")
	case <-done:
	}

	var wg sync.WaitGroup
	// now we fail the regionserver, and try to queue stuff
	mockConn.EXPECT().Close().Times(1)
	c.fail(errors.New("ooups"))
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := make(chan hrpc.RPCResult, 1)
			mockCall := mock.NewMockCall(ctrl)
			mockCall.EXPECT().ResultChan().Return(result).Times(1)
			c.QueueRPC(mockCall)
			r := <-result
			err, ok := r.Error.(UnrecoverableError)
			if !ok {
				t.Errorf("Expected UnrecoverableError error")
				return
			}
			if diff := test.Diff(ErrClientDead.error, err.error); diff != "" {
				t.Errorf("Expected: %s\nReceived: %s\nDiff:%s",
					ErrClientDead.error, err.error, diff)
			}
		}()
	}
	wg.Wait()
}
