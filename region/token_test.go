// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
)

func TestTokenBucket(t *testing.T) {
	goTake := func(ctx context.Context, s *tokenBucket) (*bool, *error) {
		var (
			done bool
			err  error
		)
		go func() {
			err = s.take(ctx)
			done = true
		}()
		return &done, &err
	}
	synctest.Test(t, func(t *testing.T) {
		s := newTokenBucket(3, nil)
		for range 3 {
			if err := s.take(context.Background()); err != nil {
				t.Fatal(err)
			}
		}

		done, err := goTake(context.Background(), s)
		synctest.Wait()
		if *done {
			t.Fatal("acquire should block")
		}
		s.release()
		synctest.Wait()
		if !*done {
			t.Fatal("acquire should have completed")
		} else if *err != nil {
			t.Fatalf("unexpected error: %s", *err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done, err = goTake(ctx, s)
		synctest.Wait()
		if *done {
			t.Fatal("acquire should have blocked")
		}
		cancel()
		synctest.Wait()
		if !*done {
			t.Fatal("acquire should have completed")
		} else if *err != context.Canceled {
			t.Errorf("unexpected error: %s", *err)
		}

		// Test resize down
		s.release()
		s.setCapacity(1)
		done, err = goTake(context.Background(), s)
		synctest.Wait()
		if *done {
			t.Fatal("acquire should have blocked")
		}
		s.release()
		synctest.Wait()
		if *done {
			t.Fatal("acquire should have blocked")
		}
		s.release()
		synctest.Wait()
		if !*done {
			t.Fatal("acquire should have completed")
		}

		// Test resize up unblocks Acquirers
		// assumes ordering when blocked on channel
		done1, err1 := goTake(context.Background(), s)
		synctest.Wait()
		done2, err2 := goTake(context.Background(), s)
		synctest.Wait()
		done3, err3 := goTake(context.Background(), s)
		synctest.Wait()
		if *done1 || *done2 || *done3 {
			t.Fatal("acquires should have blocked")
		}
		s.setCapacity(3)
		synctest.Wait()
		if !*done1 || !*done2 {
			t.Fatal("two acquires should have completed")
		}
		if *done3 {
			t.Fatal("last acquire should block")
		}
		s.release()
		synctest.Wait()
		if !*done3 {
			t.Fatal("acquire should have completed")
		}
		if *err1 != nil || *err2 != nil || *err3 != nil {
			t.Fatalf("unexpected errors: %v, %v, %v", *err1, *err2, *err3)
		}
	})
}

// TestTokenBucketRace starts multiple goroutines taking, releasing,
// and resizing a single token bucket in parallel.
func TestTokenBucketRace(t *testing.T) {
	const (
		semaMaxSize = 5

		acquirers    = 6
		acquireCount = 10

		resizers    = 2
		resizeCount = 10
	)
	s := newTokenBucket(semaMaxSize, nil)
	var wg sync.WaitGroup
	for range acquirers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range acquireCount {
				if err := s.take(context.Background()); err != nil {
					t.Error(err)
					return
				}
				s.release()
			}
		}()
	}

	for range resizers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range resizeCount {
				s.setCapacity(0)
				s.setCapacity(semaMaxSize)
			}
		}()
	}

	wg.Wait()
}

// TestTokenBucketReleaseConcurrentShrink checks the capacity after both operations
// finish. The race detector alone cannot detect a stale ballast decision.
func TestTokenBucketReleaseConcurrentShrink(t *testing.T) {
	// The ballast check and channel receive have no blocking point between
	// them, so synctest cannot force a resize into that gap. Exercise it with
	// concurrent operations instead.
	for i := range 1000 {
		s := newTokenBucket(2, nil)
		for range 2 {
			if !s.tryTake() {
				t.Fatal("initial take failed")
			}
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			s.release()
		}()
		go func() {
			defer wg.Done()
			<-start
			s.setCapacity(1)
		}()
		close(start)
		wg.Wait()

		// One token is still held, consuming the entire reduced capacity.
		if s.tryTake() {
			t.Fatalf("iteration %d: take succeeded with one token held at capacity 1", i)
		}
		s.release()
	}
}

func TestTokenBucketUnmatchedReleaseAfterShrink(t *testing.T) {
	tcs := []struct {
		name     string
		capacity int
		take     bool
	}{
		{name: "without take at zero capacity", capacity: 0},
		{name: "without take at reduced capacity", capacity: 1},
		{name: "double release at zero capacity", capacity: 0, take: true},
		{name: "double release at reduced capacity", capacity: 1, take: true},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			s := newTokenBucket(2, nil)
			if tc.take && !s.tryTake() {
				t.Fatal("initial take failed")
			}
			s.setCapacity(tc.capacity)
			if tc.take {
				s.release()
			}

			panicked := false
			func() {
				defer func() {
					panicked = recover() != nil
				}()
				s.release()
			}()
			if !panicked {
				t.Fatal("unmatched release should panic instead of consuming ballast")
			}

			// A rejected release must leave the configured capacity intact.
			for range tc.capacity {
				if !s.tryTake() {
					t.Fatal("configured capacity was lost after unmatched release")
				}
			}
			if s.tryTake() {
				t.Fatal("unmatched release increased the configured capacity")
			}
			for range tc.capacity {
				s.release()
			}
		})
	}
}
