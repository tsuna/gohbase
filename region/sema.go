// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// semaphore implements a resizable semaphore
type semaphore struct {
	c chan token

	mu sync.Mutex
	// ballast is the desired count of tokens in c that is used to
	// restrict the size of the semaphore.
	ballast int
	// curBallast is the actual number of tokens in c that is used to
	// restrict the size of the semaphore. curBallast can be lower
	// than ballast, in which case calls to Release() aids in
	// increasing ballast.
	curBallast int
}

type token = struct{}

// newSemaphore creates a new Semaphore with a given maxSize. The
// semaphore cannot be resized larger than maxSize.
func newSemaphore(maxSize int) *semaphore {
	return &semaphore{c: make(chan token, maxSize)}
}

// maxSize returns the maximum size of this semaphore
func (s *semaphore) maxSize() int {
	return cap(s.c)
}

// size returns the current size of the semaphore, set by the most recent call to Resize.
func (s *semaphore) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxSize() - s.ballast
}

// acquire blocks until there is available capacity in the semaphore.
func (s *semaphore) acquire(ctx context.Context) error {
	select {
	case s.c <- token{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release releases capacity in the semaphore. This function never blocks.
func (s *semaphore) release() {
	if s.incBallast() {
		return
	}
	select {
	case <-s.c:
	default:
		panic(errors.New("Release called more than Acquire"))
	}
}

func (s *semaphore) incBallast() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curBallast < s.ballast {
		s.curBallast++
		return true
	}
	return false
}

// resize puts a new limit on the size of the semaphore. The size must
// be between 0 and maxSize, inclusive. This function never blocks.
func (s *semaphore) resize(size int) {
	if size < 0 || size > s.maxSize() {
		panic(fmt.Errorf("resize must be between 0 and maxSize, got %d", size))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ballast = s.maxSize() - size

	// Release ballast if there is too much.
	for s.curBallast > s.ballast {
		select {
		case <-s.c:
			s.curBallast--
		default:
			panic(errors.New("Release called more than Acquire"))
		}
	}

	// Add ballast if there's not enough. Return early if adding ballast would block.
	// Any remaining ballast will be added by Release.
	for s.curBallast < s.ballast {
		select {
		case s.c <- token{}:
			s.curBallast++
		default:
			return
		}
	}
}
