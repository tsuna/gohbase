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

	"github.com/mihkulemin/token"
	"github.com/tsuna/gohbase/compression"
	"github.com/tsuna/gohbase/hrpc"
)

// RegionClientOptions holds configuration options for RegionClient
type RegionClientOptions struct {
	// QueueSize sets the RPC queue size for batching requests
	QueueSize int
	// FlushInterval sets the flush interval for batching RPCs
	FlushInterval time.Duration
	// EffectiveUser sets the effective user for the connection
	EffectiveUser string
	// ReadTimeout sets the read timeout for RPCs
	ReadTimeout time.Duration
	// Codec sets the compression codec for cellblocks
	Codec compression.Codec
	// Dialer sets a custom dialer for connecting to region servers
	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)
	// Logger sets a custom logger
	Logger *slog.Logger
	// ScanControl enables congestion control for scan requests
	ScanControl *ScanControlOptions
}

// ScanControlOptions holds scan congestion control configuration
type ScanControlOptions struct {
	// NewController is a factory function that creates controllers with min/max window bounds
	NewController NewControllerFunc
	// MinWindow sets the minimum window size (concurrency level)
	MinWindow int
	// MaxWindow sets the maximum window size (concurrency level)
	MaxWindow int
	// Interval sets the interval between ping scans
	Interval time.Duration
}

// NewClient creates a new RegionClient with RegionClientOptions.
func NewClient(addr string, ctype ClientType, options *RegionClientOptions) hrpc.RegionClient {
	c := &client{
		addr:          addr,
		ctype:         ctype,
		rpcQueueSize:  DefaultRPCQueueSize,
		flushInterval: DefaultFlushInterval,
		effectiveUser: DefaultEffectiveUser,
		readTimeout:   DefaultReadTimeout,
		rpcs:          make(chan []hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
	}

	// Set default dialer
	var d net.Dialer
	c.dialer = d.DialContext

	// Set default logger
	c.logger = slog.Default()

	// Apply options if provided
	if options != nil {
		if options.QueueSize > 0 {
			c.rpcQueueSize = options.QueueSize
		}
		if options.FlushInterval > 0 {
			c.flushInterval = options.FlushInterval
		}
		if options.EffectiveUser != "" {
			c.effectiveUser = options.EffectiveUser
		}
		if options.ReadTimeout > 0 {
			c.readTimeout = options.ReadTimeout
		}
		if options.Codec != nil {
			c.compressor = &compressor{Codec: options.Codec}
		}
		if options.Dialer != nil {
			c.dialer = options.Dialer
		}
		if options.Logger != nil {
			c.logger = options.Logger
		}
		if options.ScanControl != nil && options.ScanControl.NewController != nil {
			// Create context for token bucket that will be cancelled when client closes
			ctx, cancel := context.WithCancelCause(context.Background())

			// Store min/max window values
			c.scanMinWindow = options.ScanControl.MinWindow
			c.scanMaxWindow = options.ScanControl.MaxWindow

			// Create controller instance for this region client
			c.scanController = options.ScanControl.NewController(c.scanMinWindow, c.scanMaxWindow)

			// Always start with minimum window
			initialWindow := c.scanMinWindow

			// Create token bucket with given capacity
			tb, err := token.NewToken(ctx, c.scanMaxWindow, c.scanMinWindow)
			if err != nil {
				c.logger.Error("failed to create scan token bucket", "error", err)
				cancel(nil) // cancel the context, as it is not needed in the case of error
			} else {
				// Cancel context when client is done
				go func() {
					<-c.done
					cancel(ErrClientClosed)
				}()

				c.scanTokenBucket = tb
				c.pingInterval = options.ScanControl.Interval
				c.scanTokenBucket.SetCapacity(context.Background(), initialWindow)
				concurrentScans.WithLabelValues(c.addr).Set(float64(initialWindow))
			}
		}
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
				go c.controlLoop() // Ping goroutine
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
