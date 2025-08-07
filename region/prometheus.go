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

	windowSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gohbase",
			Name:      "congestion_window",
			Help:      "Window size in number of requests for congestion control",
		},
		[]string{"regionserver"},
	)
)
