// Copyright (C) 2021  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"net"

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

	bytesReadTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gohbase",
			Name:      "bytes_read_total",
			Help:      "Bytes read from hbase",
		},
		[]string{"regionserver"},
	)

	bytesWrittenTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gohbase",
			Name:      "bytes_written_total",
			Help:      "Bytes written to hbase",
		},
		[]string{"regionserver"},
	)
)

type promMetricConn struct {
	net.Conn
	counterRead  prometheus.Counter
	counterWrite prometheus.Counter
}

func (mc promMetricConn) Read(b []byte) (int, error) {
	n, err := mc.Conn.Read(b)
	mc.counterRead.Add(float64(n))
	return n, err
}

func (mc promMetricConn) Write(b []byte) (int, error) {
	n, err := mc.Conn.Write(b)
	mc.counterWrite.Add(float64(n))
	return n, err
}
