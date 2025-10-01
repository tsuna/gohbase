package region

import (
	"context"
	"errors"
	"fmt"
)

var (
	tokenClosedErr = errors.New("token bucket closed")
)

// Token represents a token bucket with dynamic capacity adjustment.
// It uses channels for thread-safe token management
type Token struct {
	buf chan struct{}
	// Maximum number of possible token
	capacity int
	// Channel for capacity changes
	change chan int
	// Actual number of tokens available
	number int
	// Channel to close tokens
	done chan struct{}
}

// NewToken creates a new Token bucket with the specified maximum capacity and initial length.
// done channel used to stop manager goroutine.
func NewToken(cap, num int, done chan struct{}) (*Token, error) {
	if cap <= 0 || num < 0 || cap < num {
		return nil, fmt.Errorf("incorrect max capacity (%d) and/or length (%d)", cap, num)
	}

	t := &Token{
		buf:      make(chan struct{}, cap),
		capacity: cap,
		change:   make(chan int),
		number:   num,
		done:     done,
	}

	for i := 0; i < t.number; i++ {
		t.buf <- struct{}{}
	}

	go t.manager()

	return t, nil
}

// Take acquires a token from the bucket, blocking until one is available
// or the context is cancelled. If done is closed Take always returns nil.
func (t *Token) Take(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.done:
		return nil
	case <-t.buf:
		return nil
	}
}

// Release returns a token to the bucket, blocking if the bucket is full
// until space is available or the context is cancelled. If done is closed Take always returns nil.
func (t *Token) Release(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.done:
		return nil
	case t.buf <- struct{}{}:
		return nil

	}
}

// SetCapacity dynamically adjusts the number of available tokens in the bucket.
// The new capacity must be between 0 and the maximum capacity set during initialization.
// The adjustment is handled asynchronously by the internal manager goroutine.
func (t *Token) SetCapacity(ctx context.Context, c int) error {
	if c > t.capacity || c < 0 {
		return fmt.Errorf("capacity (%d) should be between 0 and maximum (%d)", c, t.capacity)
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.done:
		return tokenClosedErr
	case t.change <- c:
	}
	return nil
}

// manager is an internal goroutine that handles dynamic capacity adjustments.
// It continuously monitors for capacity change requests and adjusts the number
// of tokens in the bucket accordingly by either adding or removing tokens.
func (t *Token) manager() {
	var in chan<- struct{}
	var out <-chan struct{}
	target := t.number

	for {
		select {
		case <-t.done:
			return
		case in <- struct{}{}:
			t.number++
		case <-out:
			t.number--
		case target = <-t.change:
		}

		if target > t.number {
			in = t.buf
			out = nil
		} else if target < t.number {
			in = nil
			out = t.buf
		} else {
			in = nil
			out = nil
		}
	}
}
