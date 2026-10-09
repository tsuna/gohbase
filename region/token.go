package region

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	tokenClosedErr = errors.New("token bucket closed")
)

// tokenBucket represents a token bucket with a dynamically adjustable capacity.
type tokenBucket struct {
	done <-chan struct{}

	sema chan token

	mu sync.Mutex
	// ballast is the desired count of tokens to restrict the size of sema.
	ballast int
	// curBallast is the actual number of ballast tokens in sema.
	// curBallast can be lower than ballast, in which case calls
	// to release() aid in increasing ballast.
	curBallast int
}

type token = struct{}

// newTokenBucket creates a token bucket initially at the specified maximum capacity.
// Use setCapacity to change the capacity. Closing done stops blocked operations.
func newTokenBucket(cap int, done chan struct{}) *tokenBucket {
	return &tokenBucket{done: done, sema: make(chan token, cap)}
}

func (t *tokenBucket) maxSize() int {
	return cap(t.sema)
}

// tryTake attempts to acquire a token without blocking.
func (t *tokenBucket) tryTake() bool {
	select {
	case t.sema <- token{}:
		return true
	default:
		return false
	}
}

// take acquires a token, blocking until one is available or the context is
// canceled. If done is closed, take returns tokenClosedErr.
func (t *tokenBucket) take(ctx context.Context) error {
	select {
	case t.sema <- token{}:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.done:
		return tokenClosedErr
	}
}

// release returns a token to the bucket. It panics when called without a
// previous successful take or tryTake.
func (t *tokenBucket) release() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// len(t.sema) represents the amount of sema that is occupied. It should never be below the
	// count of ballast, otherwise that indicates release has been called without a prior call to
	// take/tryTake.
	if len(t.sema) <= t.curBallast {
		panic(errors.New("release called more than take"))
	}

	// A shrink may need more ballast than setCapacity could insert while tokens
	// were held. Convert this acquired token into ballast without removing its
	// channel entry, so releasing it does not admit another take too early.
	if t.incBallast() {
		return
	}
	<-t.sema
}

func (t *tokenBucket) incBallast() bool {
	if t.curBallast < t.ballast {
		t.curBallast++
		return true
	}
	return false
}

// setCapacity changes the number of available tokens. The new capacity must
// be between 0 and the maximum capacity set during initialization.
func (t *tokenBucket) setCapacity(size int) {
	if size < 0 || size > t.maxSize() {
		panic(fmt.Errorf("resize must be between 0 and maxSize, got %d", size))
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ballast = t.maxSize() - size

	// Remove ballast if there is too much.
	for t.curBallast > t.ballast {
		select {
		case <-t.sema:
			t.curBallast--
		default:
			panic(errors.New("release called more than take"))
		}
	}

	// Add ballast if there's not enough. Return early if adding ballast would block.
	// Any remaining ballast will be added by release.
	for t.curBallast < t.ballast {
		select {
		case t.sema <- token{}:
			t.curBallast++
		default:
			return
		}
	}
}
