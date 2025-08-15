// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

//go:build !testing

package region

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/tsuna/gohbase/compression"
	"github.com/tsuna/gohbase/hrpc"
)

// Option is a function that modifies a client
type Option func(*client)

// WithQueueSize sets the RPC queue size
func WithQueueSize(size int) Option {
	return func(c *client) {
		c.rpcQueueSize = size
	}
}

// WithFlushInterval sets the flush interval for batching RPCs
func WithFlushInterval(interval time.Duration) Option {
	return func(c *client) {
		c.flushInterval = interval
	}
}

// WithEffectiveUser sets the effective user for the connection
func WithEffectiveUser(user string) Option {
	return func(c *client) {
		c.effectiveUser = user
	}
}

// WithReadTimeout sets the read timeout for RPCs
func WithReadTimeout(timeout time.Duration) Option {
	return func(c *client) {
		c.readTimeout = timeout
	}
}

// WithCodec sets the compression codec for cellblocks
func WithCodec(codec compression.Codec) Option {
	return func(c *client) {
		if codec != nil {
			c.compressor = &compressor{Codec: codec}
		}
	}
}

// WithDialer sets a custom dialer for connecting to region servers
func WithDialer(dialer func(ctx context.Context, network, addr string) (net.Conn, error)) Option {
	return func(c *client) {
		if dialer != nil {
			c.dialer = dialer
		}
	}
}

// WithLogger sets a custom logger
func WithLogger(logger *slog.Logger) Option {
	return func(c *client) {
		c.logger = logger
	}
}

// WithPingInterval sets the interval between ping scans. Set to 0 to disable pinging.
func WithPingInterval(interval time.Duration) Option {
	return func(c *client) {
		c.pingInterval = interval
	}
}

// WithPingLatencyWindow sets the number of latest ping measurements to keep for calculating average
func WithPingLatencyWindow(window int) Option {
	return func(c *client) {
		if window > 0 {
			c.pingLatencyWindow = window
			c.pingLatencies = make([]time.Duration, 0, window)
		}
	}
}

// NewClient creates a new RegionClient with options.
func NewClient(addr string, ctype ClientType, opts ...Option) hrpc.RegionClient {
	c := &client{
		addr:              addr,
		ctype:             ctype,
		rpcQueueSize:      DefaultRPCQueueSize,
		flushInterval:     DefaultFlushInterval,
		effectiveUser:     DefaultEffectiveUser,
		readTimeout:       DefaultReadTimeout,
		rpcs:              make(chan []hrpc.Call),
		done:              make(chan struct{}),
		sent:              make(map[uint32]hrpc.Call),
		pingInterval:      0, // disabled by default
		pingLatencyWindow: DefaultPingLatencyWindow,
		pingLatencies:     make([]time.Duration, 0, DefaultPingLatencyWindow),
	}

	// Set default dialer
	var d net.Dialer
	c.dialer = d.DialContext

	// Set default logger
	c.logger = slog.Default()

	// Apply all options
	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *client) Dial(ctx context.Context) error {
	c.dialOnce.Do(func() {
		conn, err := c.dialer(ctx, "tcp", c.addr)
		if err != nil {
			c.fail(fmt.Errorf("failed to dial RegionServer: %s", err))
			return
		}

		c.connM.Lock()
		c.conn = conn
		c.connM.Unlock()

		// time out send hello if it take long
		if deadline, ok := ctx.Deadline(); ok {
			if err = c.conn.SetWriteDeadline(deadline); err != nil {
				c.fail(fmt.Errorf("failed to set write deadline: %s", err))
				return
			}
		}
		if err := c.sendHello(); err != nil {
			c.fail(fmt.Errorf("failed to send hello to RegionServer: %s", err))
			return
		}
		// reset write deadline
		if err = c.conn.SetWriteDeadline(time.Time{}); err != nil {
			c.fail(fmt.Errorf("failed to set write deadline: %s", err))
			return
		}

		if c.ctype == RegionClient {
			go c.processRPCs() // Batching goroutine
			if c.pingInterval > 0 {
				go c.pingLoop() // Ping goroutine
			}
		}
		go c.receiveRPCs() // Reader goroutine
	})

	select {
	case <-c.done:
		return ErrClientClosed
	default:
		return nil
	}
}
