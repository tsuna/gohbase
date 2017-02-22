// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build !go1.7,!testing

package region

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

type clientResult struct {
	Client *client
	Err    error
}

// NewClient creates a new RegionClient.
func NewClient(ctx context.Context, host string, port uint16, ctype ClientType,
	queueSize int, flushInterval time.Duration, effectiveUser string) (hrpc.RegionClient, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(int(port)))
	ch := make(chan *clientResult, 1)
	go func() {
		defer close(ch)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			ch <- &clientResult{
				Err: fmt.Errorf("failed to connect to the RegionServer at %s: %s", addr, err),
			}
			return
		}
		c := &client{
			host:          host,
			port:          port,
			conn:          conn,
			rpcs:          make(chan hrpc.Call),
			done:          make(chan struct{}),
			sent:          make(map[uint32]hrpc.Call),
			rpcQueueSize:  queueSize,
			flushInterval: flushInterval,
			effectiveUser: effectiveUser,
		}
		if err := c.sendHello(ctype); err != nil {
			conn.Close()
			ch <- &clientResult{
				Err: fmt.Errorf("failed to send hello to the RegionServer at %s: %s", addr, err),
			}
			return
		}
		ch <- &clientResult{Client: c}
	}()
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		c := res.Client
		go c.processRPCs() // Writer goroutine
		go c.receiveRPCs() // Reader goroutine
		return c, nil
	case <-ctx.Done():
		return nil, errors.New("deadline exceeded")
	}
}
