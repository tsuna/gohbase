package observability

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// noopTraceSpan to represent a span with no operation (do nothing)
type noopTraceSpan struct{}

// AddEvent implements trace.Span
func (noopTraceSpan) AddEvent(name string, options ...trace.EventOption) {
	// do nothing
}

// End implements trace.Span
func (noopTraceSpan) End(options ...trace.SpanEndOption) {
	// do nothing
}

// IsRecording implements trace.Span
func (noopTraceSpan) IsRecording() bool {
	return false
}

// RecordError implements trace.Span
func (noopTraceSpan) RecordError(err error, options ...trace.EventOption) {
	// do nothing
}

// SetAttributes implements trace.Span
func (noopTraceSpan) SetAttributes(kv ...attribute.KeyValue) {
	// do nothing
}

// SetName implements trace.Span
func (noopTraceSpan) SetName(name string) {
	// do nothing
}

// SetStatus implements trace.Span
func (noopTraceSpan) SetStatus(code codes.Code, description string) {
	// do nothing
}

// SpanContext implements trace.Span
func (noopTraceSpan) SpanContext() trace.SpanContext {
	// do nothing
	return trace.SpanContext{}
}

// TracerProvider implements trace.Span
func (noopTraceSpan) TracerProvider() trace.TracerProvider {
	// noop
	return trace.NewNoopTracerProvider()
}

func newNoopSpan() trace.Span {
	return noopTraceSpan{}
}

var (
	isNoopTracing = false
)

// WithNoopTracing enable noop tracing operation
// it is used when you don't want to observability
func WithNoopTracing() {
	isNoopTracing = true
}
