// Copyright (C) 2021  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsuna/gohbase/pb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// RequestTracePropagator is used to propagate
// tracing information into the RPCTInfo field
// of the RequestHeader
type RequestTracePropagator struct {
	RequestHeader *pb.RequestHeader
}

// StartSpan starts a trace with the given context
func StartSpan(
	ctx context.Context,
	name string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	tracer := otel.GetTracerProvider().Tracer("gohbase")
	return tracer.Start(ctx, name, opts...)
}

// ObserveWithTrace observes the value, providing the traceID as
// an exemplar if exemplars are supported and a traceID is present
func ObserveWithTrace(ctx context.Context, o prometheus.Observer, v float64) {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsSampled() && spanContext.HasTraceID() {
		traceID := spanContext.TraceID().String()

		if exemplarObserver, ok := o.(prometheus.ExemplarObserver); ok {
			exemplarObserver.ObserveWithExemplar(v, prometheus.Labels{
				"traceID": traceID,
			})
			return
		}
	}

	o.Observe(v)
}

// Get implements the go.opentelemetry.io/otel/propagation.TextMapCarrier interface
func (r RequestTracePropagator) Get(key string) string {
	if r.RequestHeader == nil ||
		r.RequestHeader.TraceInfo == nil ||
		r.RequestHeader.TraceInfo.Headers == nil {
		return ""
	}
	return r.RequestHeader.TraceInfo.Headers[key]
}

// Set implements the go.opentelemetry.io/otel/propagation.TextMapCarrier interface
func (r RequestTracePropagator) Set(key string, value string) {
	if r.RequestHeader == nil {
		return
	}
	if r.RequestHeader.TraceInfo == nil {
		r.RequestHeader.TraceInfo = &pb.RPCTInfo{
			Headers: make(map[string]string),
		}
	}
	r.RequestHeader.TraceInfo.Headers[key] = value
}

// Keys implements the go.opentelemetry.io/otel/propagation.TextMapCarrier interface
func (r RequestTracePropagator) Keys() []string {
	if r.RequestHeader == nil ||
		r.RequestHeader.TraceInfo == nil ||
		r.RequestHeader.TraceInfo.Headers == nil {
		return []string{}
	}

	h := r.RequestHeader.TraceInfo.Headers
	result := make([]string, 0, len(h))
	for k := range h {
		result = append(result, k)
	}
	return result
}
