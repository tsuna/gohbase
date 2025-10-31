// Copyright (C) 2021  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	flushReasonCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gohbase",
			Name:      "batch_flush_count",
			Help:      "Number of times a gohbase batch was flushed",
		},
		[]string{"reason"},
	)

	flushSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gohbase",
			Name:      "batch_flush_size",
			Help:      "Number of RPCs sent in multis",
			Buckets:   prometheus.ExponentialBuckets(1, 5, 8),
		},
		[]string{"regionserver"},
	)

	rpcSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gohbase",
			Name:      "rpc_size_bytes",
			Help:      "Number of bytes sent per RPC call to HBase",
			// >>> [1024*(4**i) for i in range(8)]
			// [1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216]
			Buckets: prometheus.ExponentialBuckets(1024, 4, 8),
		},
		[]string{"regionserver"},
	)

	rpcResultCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gohbase",
			Name:      "rpc_result_count",
			Help:      "Number of RPC operations by result status and operation type",
		},
		[]string{"operation", "status", "type"},
	)

	pingLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gohbase",
			Name:      "ping_latency_seconds",
			Help:      "Ping scan latency in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0002, 2, 12),
		},
		[]string{"regionserver"},
	)

	concurrentScans = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gohbase",
			Name:      "concurrent_scans_limit",
			Help:      "Max number of concurrent scans per region server",
		},
		[]string{"regionserver"},
	)
)
