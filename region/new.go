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
	// BatchRequestsControl enables concurrency control for batch requests
	BatchRequestsControl *BatchRequestsControlOptions
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

// BatchRequestsControlOptions holds batch requests concurrency control configuration.
type BatchRequestsControlOptions struct {
	// MaxConcurrency sets the maximum number of concurrent batch requests
	MaxConcurrency int
}

// NewClient creates a new RegionClient with RegionClientOptions.
func NewClient(addr string, ctype ClientType, opts *RegionClientOptions) hrpc.RegionClient {
	c := &client{
		addr:          addr,
		ctype:         ctype,
		rpcQueueSize:  DefaultRPCQueueSize,
		flushInterval: DefaultFlushInterval,
		effectiveUser: DefaultEffectiveUser,
		readTimeout:   DefaultReadTimeout,
		dialer:        defaultDialer.DialContext,
		logger:        slog.Default(),
		rpcs:          make(chan []hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
	}

	// Apply options if provided
	if opts != nil {
		if opts.QueueSize > 0 {
			c.rpcQueueSize = opts.QueueSize
		}
		if opts.FlushInterval > 0 {
			c.flushInterval = opts.FlushInterval
		}
		if opts.EffectiveUser != "" {
			c.effectiveUser = opts.EffectiveUser
		}
		if opts.ReadTimeout > 0 {
			c.readTimeout = opts.ReadTimeout
		}
		if opts.Codec != nil {
			c.compressor = &compressor{Codec: opts.Codec}
		}
		if opts.Dialer != nil {
			c.dialer = opts.Dialer
		}
		if opts.Logger != nil {
			c.logger = opts.Logger
		}

		if err := c.configScanControl(opts.ScanControl); err != nil {
			c.logger.Warn("scan control disabled due to invalid configuration", "error", err)
		}

		if err := c.configBatchRequestsControl(opts.BatchRequestsControl); err != nil {
			c.logger.Warn("batch requests control disabled due to invalid configuration",
				"error", err)
		}
	}
	return c
}

func (c *client) configScanControl(opts *ScanControlOptions) error {
	var err error

	if opts == nil {
		return nil
	}

	c.scanController, err = opts.NewController(opts.MinWindow, opts.MaxWindow)
	if err != nil {
		return err
	}

	// Initial window depends on the particular controller
	initialWindow := c.scanController.Window()

	// Create token bucket with given capacity
	tb, err := NewToken(opts.MaxWindow, opts.MinWindow, c.done)
	if err != nil {
		return err
	}

	if opts.Interval <= 0 {
		return fmt.Errorf("interval must be greater than 0, got %v", opts.Interval)
	}

	c.pingInterval = opts.Interval
	c.scanTokenBucket = tb
	c.scanTokenBucket.SetCapacity(context.Background(), initialWindow)
	concurrentScans.WithLabelValues(c.addr).Set(float64(initialWindow))
	return nil
}

func (c *client) configBatchRequestsControl(opts *BatchRequestsControlOptions) error {
	if opts == nil {
		return nil
	}

	if opts.MaxConcurrency <= 0 {
		return fmt.Errorf("max concurrency must be greater than 0, got %d", opts.MaxConcurrency)
	}

	tb, err := NewToken(opts.MaxConcurrency, opts.MaxConcurrency, c.done)
	if err != nil {
		return err
	}

	c.batchRequestsTokenBucket = tb
	concurrentBatchRequests.WithLabelValues(c.addr).Set(float64(opts.MaxConcurrency))
	return nil
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
				go c.controlLoop() // Ping goroutine for Scan concurrency control
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
