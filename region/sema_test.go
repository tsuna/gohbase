// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Limiting to go1.25 because this test uses testing/synctest, only available in go1.25
//go:build go1.25

package region

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
)

func TestSemaphore(t *testing.T) {
	goAcquire := func(ctx context.Context, s *semaphore) (*bool, *error) {
		var (
			done bool
			err  error
		)
		go func() {
			err = s.acquire(ctx)
			done = true
		}()
		return &done, &err
	}
	synctest.Test(t, func(t *testing.T) {
		s := newSemaphore(3)
		s.acquire(context.Background())
		s.acquire(context.Background())
		s.acquire(context.Background())

		done, err := goAcquire(context.Background(), s)
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
		done, err = goAcquire(ctx, s)
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
		s.resize(1)
		done, err = goAcquire(context.Background(), s)
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
		done1, err1 := goAcquire(context.Background(), s)
		synctest.Wait()
		done2, err2 := goAcquire(context.Background(), s)
		synctest.Wait()
		done3, err3 := goAcquire(context.Background(), s)
		synctest.Wait()
		if *done1 || *done2 || *done3 {
			t.Fatal("acquires should have blocked")
		}
		s.resize(3)
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

// TestRace starts multiple goroutines acquiring, releasing,
// and resizing a single semaphore in parallel.
func TestRace(t *testing.T) {
	const (
		semaMaxSize = 5

		acquirers    = 6
		acquireCount = 10

		resizers    = 2
		resizeCount = 10
	)
	s := newSemaphore(semaMaxSize)
	var wg sync.WaitGroup
	for range acquirers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range acquireCount {
				s.acquire(context.Background())
				s.release()
			}
		}()
	}

	for range resizers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range resizeCount {
				s.resize(0)
				s.resize(semaMaxSize)
			}
		}()
	}

	wg.Wait()
}
