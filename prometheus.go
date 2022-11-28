// Copyright (C) 2021  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var operationDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "gohbase",
		Name:      "operation_duration_seconds",
		Help:      "Time in seconds for operation to complete",
		//   >>> [0.04*(2**i) for i in range(11)]
		//   [0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56, 5.12, 10.24, 20.48, 40.96]
		// Meaning 40ms, 80ms, 160ms, 320ms, 640ms, 1.28s, ... max 40.96s
		// (most requests have a 30s timeout by default at the Envoy level)
		Buckets: prometheus.ExponentialBuckets(0.04, 2, 11),
	},
	[]string{"operation", "result"},
)
