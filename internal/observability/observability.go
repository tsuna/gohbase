// Copyright (C) 2021  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// StartSpan starts a trace with the given context
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracer := otel.GetTracerProvider().Tracer("gohbase")
	return tracer.Start(ctx, name, opts...)
}

// ObserveWithTrace observes the value, providing the traceID as
// an exemplar if exemplars are supported and a traceID is present
func ObserveWithTrace(ctx context.Context, o prometheus.Observer, v float64) {
	spanContext := trace.SpanContextFromContext(ctx)
	if exemplarObserver, ok := o.(prometheus.ExemplarObserver); ok && spanContext.HasTraceID() {
		exemplarObserver.ObserveWithExemplar(v, prometheus.Labels{
			"traceID": spanContext.TraceID().String(),
		})
		return
	}

	o.Observe(v)
}
