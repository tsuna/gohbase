// Copyright (C) 2024  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bufio"
	"io"
	"sync"

	"github.com/tsuna/gohbase/hrpc"
	"google.golang.org/protobuf/proto"
)

type congestionControl struct {
	c *client

	// retry allows the readLoop to send failed RPCs back to the write
	// loop for retrying. They are prioritized over new requests.
	retry chan hrpc.Call

	// sendWindow is the dynamic limit on the number of outstanding
	// requests.
	sendWindow int
	minWindow  int
	maxWindow  int
	// sema limits the total number of outstanding requests. Before
	// sending a request a token must be pushed into the sema. If
	// there isn't space then it blocks. Requests that are received
	// pull a token out of the sema. Tokens are pushed into the sema
	// in writeLoop and pulled in readLoop.
	sema *semaphore

	// IO functions overridable for testing
	trySend func(hrpc.Call) error
	receive func(io.Reader) (hrpc.Call, proto.Message, error)
}

func newCongestion(c *client, minWindowSize, maxWindowSize int) *congestionControl {
	cc := &congestionControl{
		c: c,
		// retry is buffered to maxWindowSize to make it unlikely it
		// fills
		retry: make(chan hrpc.Call, maxWindowSize),

		sendWindow: max(minWindowSize, maxWindowSize/2),
		minWindow:  minWindowSize,
		maxWindow:  maxWindowSize,

		trySend: c.trySend,
		receive: c.receive,
	}
	cc.sema = newSemaphore(cc.sendWindow, maxWindowSize)

	return cc
}

func (cc *congestionControl) run(req <-chan hrpc.Call, done <-chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cc.writeLoop(req, done)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		cc.readLoop()
	}()
	wg.Wait()
}

func (cc *congestionControl) send(rpc hrpc.Call) error {
	cc.sema.take1()
	if err := cc.trySend(rpc); err != nil {
		returnResult(rpc, nil, err)
		if _, ok := err.(ServerError); ok {
			// trySend will fail the client
			return err
		}
		cc.sema.release1()
	}
	return nil
}

func (cc *congestionControl) writeLoop(req <-chan hrpc.Call, done <-chan struct{}) {
	for {
		// Prioritize retry requests
		select {
		case rpc := <-cc.retry:
			if err := cc.send(rpc); err != nil {
				return
			}
		case <-done:
			return
		default:
		}

		select {
		case rpc := <-req:
			if err := cc.send(rpc); err != nil {
				return
			}
		case rpc := <-cc.retry:
			if err := cc.send(rpc); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (cc *congestionControl) read(r io.Reader) error {
	// TODO: Requests with priority shouldn't affect the sema or send window
	rpc, resp, err := cc.receive(r)
	if err != nil {
		if _, ok := err.(ServerError); ok {
			returnResult(rpc, resp, err)
			return err
		}
		if _, ok := err.(RetryableError); ok {
			newSendWindow := cc.sendWindow / 2
			if newSendWindow < cc.minWindow {
				newSendWindow = cc.minWindow
			}
			if newSendWindow != cc.sendWindow {
				// Get the semaphore in line with our window size by
				// adding the difference between the newSendWindow and
				// the existing sendWindow.
				diff := newSendWindow - cc.sendWindow
				cc.sema.add(diff)
				cc.windowDecreases.Add(float64(-diff))
				cc.sendWindow = newSendWindow
			}
			cc.sema.release1()
			// Prioritize this request by putting it on cc.retry.
			select {
			case cc.retry <- rpc:
			default:
				// read cannot block or else it may result in a
				// deadlock between the writer and reader. If cc.retry
				// doesn't have room for this request return the
				// result to the caller to retry.
				returnResult(rpc, resp, err)
			}
			return nil
		}
		// other errors will be returned to the client to be handled
	}
	returnResult(rpc, resp, err)

	// Request succeeded or hit an error unrelated to congestion, so
	// expand sendWindow
	cc.sendWindow = min(cc.sendWindow+1, cc.maxWindow)
	// increase the sema by 2, (1 for the received response and 1
	// because the window size has increased)
	cc.sema.add(2)
	return nil
}

func (cc *congestionControl) readLoop() {
	defer cc.sema.add(cc.maxWindow * 2) // unblock a possible waiter in writeLoop
	r := bufio.NewReader(cc.c.conn)
	for {
		if err := cc.read(r); err != nil {
			// fail the client and let the callers establish a new one
			cc.c.fail(err)
			return
		}
	}
}

// semaphore implements a semaphore using a condition variable and
// mutex. This is more commonly implemented in Go with a buffered
// channel, but a buffered channel doesn't allow consuming multiple
// tokens at once or going negative in the number of consumed tokens
// which this semaphore does. These abilities make it simpler to
// reduce the window size after an error.
type semaphore struct {
	c sync.Cond
	m sync.Mutex
	v int

	max int
}

func newSemaphore(v int, max int) *semaphore {
	s := &semaphore{v: v, max: max}
	s.c.L = &s.m
	return s
}

// waits for v to be positive and then subtracts 1
func (s *semaphore) take1() {
	s.m.Lock()
	defer s.m.Unlock()
	for s.v <= 0 {
		s.c.Wait()
	}
	s.v--
}

func (s *semaphore) release1() {
	s.add(1)
}

func (s *semaphore) add(v int) {
	s.m.Lock()
	defer s.m.Unlock()
	prev := s.v
	s.v = min(s.v+v, s.max)
	if prev <= 0 && s.v > 0 {
		// because we have a max of one waiter (the writeLoop
		// goroutine) we just need to signal once when s.v becomes
		// positive.
		s.c.Signal()
	}
}
