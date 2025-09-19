// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"testing"
	"time"
)

// TestConcurrencyIncreaseOnLowLatency tests that concurrency increases by 1
// when latency is below the low threshold
func TestConcurrencyIncreaseOnLowLatency(t *testing.T) {
	newControllerFunc := NewAIMDController(100*time.Millisecond, 200*time.Millisecond)
	controller := newControllerFunc(5, 50)

	// First increase - from 5 to 6
	newConcurrency, changed := controller.Latency(50*time.Millisecond)
	if newConcurrency != 6 {
		t.Errorf("Expected concurrency to increase to 6, got %d", newConcurrency)
	}
	if !changed {
		t.Errorf("Expected value to change on first increase")
	}
	
	// Second increase - from 6 to 7 (confirm linearity)
	newConcurrency, changed = controller.Latency(50*time.Millisecond)
	if newConcurrency != 7 {
		t.Errorf("Expected concurrency to increase to 7, got %d", newConcurrency)
	}
	if !changed {
		t.Errorf("Expected value to change on second increase")
	}
	
	// Continue increasing until beyond maxConcurrency
	for i := 0; i < 50; i++ {
		controller.Latency(50*time.Millisecond) // Low latency (below threshold of 100ms)
	}
	
	// Should be at maxConcurrency now
	if controller.Window() != 50 {
		t.Errorf("Expected concurrency to be at max (50), got %d", controller.Window())
	}
}

// TestConcurrencyDecreaseOnHighLatency tests that concurrency decreases by decreaseDelta
// when latency is above the high threshold
func TestConcurrencyDecreaseOnHighLatency(t *testing.T) {
	newControllerFunc := NewAIMDController(100*time.Millisecond, 200*time.Millisecond)
	controller := newControllerFunc(5, 50)
	
	// Start by increasing concurrency to a higher value
	for i := 0; i < 20; i++ {
		controller.Latency(50*time.Millisecond) // Low latency to increase concurrency
	}
	
	// Should be at 25 (5 + 20)
	if controller.Window() != 25 {
		t.Fatalf("Expected concurrency to be 25, got %d", controller.Window())
	}
	
	// First decrease - should decrease by 1 (initial delta)
	newConcurrency, changed := controller.Latency(250*time.Millisecond) // High latency (above threshold of 200ms)
	if newConcurrency != 24 {
		t.Errorf("Expected concurrency to decrease to 24, got %d", newConcurrency)
	}
	if !changed {
		t.Errorf("Expected value to change on first decrease")
	}
	
	// Second decrease - delta doubles to 2, so 24 -> 22
	newConcurrency, changed = controller.Latency(250*time.Millisecond)
	if newConcurrency != 22 {
		t.Errorf("Expected concurrency to decrease to 22, got %d", newConcurrency)
	}
	if !changed {
		t.Errorf("Expected value to change on second decrease")
	}
	
	// Third decrease - delta doubles to 4, so 22 -> 18
	newConcurrency, changed = controller.Latency(250*time.Millisecond)
	if newConcurrency != 18 {
		t.Errorf("Expected concurrency to decrease to 18, got %d", newConcurrency)
	}
	if !changed {
		t.Errorf("Expected value to change on third decrease")
	}
	
	// Continue decreasing until we hit minConcurrency
	for i := 0; i < 30; i++ {
		controller.Latency(250*time.Millisecond)
	}
	
	// Should be at minConcurrency now
	if controller.Window() != 5 {
		t.Errorf("Expected concurrency to be at min (5), got %d", controller.Window())
	}
}

// TestConcurrencyNoChangeInMiddle tests that concurrency doesn't change
// when latency is between low and high thresholds
func TestConcurrencyNoChangeInMiddle(t *testing.T) {
	newControllerFunc := NewAIMDController(100*time.Millisecond, 200*time.Millisecond)
	controller := newControllerFunc(5, 50)
	
	// Increase concurrency to a middle value
	for i := 0; i < 10; i++ {
		controller.Latency(50*time.Millisecond) // Low latency
	}
	
	// Should be at 15 (5 + 10)
	if controller.Window() != 15 {
		t.Fatalf("Expected concurrency to be 15, got %d", controller.Window())
	}
	
	// Multiple reports with latency between thresholds - should stay the same
	for i := 0; i < 10; i++ {
		newConcurrency, changed := controller.Latency(150*time.Millisecond) // Between 100ms and 200ms
		if changed {
			t.Errorf("Expected no change for middle latency, but value changed to %d", newConcurrency)
		}
	}
	
	if controller.Window() != 15 {
		t.Errorf("Expected concurrency to remain at 15 after multiple middle latencies, got %d", controller.Window())
	}
}

// TestDecreaseDeltaDoublingAndHalving tests that decreaseDelta doubles when decreasing
// and halves when increasing or stable
func TestDecreaseDeltaDoublingAndHalving(t *testing.T) {
	newControllerFunc := NewAIMDController(100*time.Millisecond, 200*time.Millisecond)
	controller := newControllerFunc(5, 50)
	
	// Start by increasing concurrency to 30
	for i := 0; i < 25; i++ {
		controller.Latency(50*time.Millisecond)
	}

	if controller.Window() != 30 {
		t.Fatalf("Expected concurrency to be 30, got %d", controller.Window())
	}

	// High latency - decrease by 1 (initial delta)
	newConcurrency, _ := controller.Latency(250*time.Millisecond)
	if newConcurrency != 29 {
		t.Errorf("Expected concurrency to decrease to 29, got %d", newConcurrency)
	}

	// High latency again - delta doubles to 2
	newConcurrency, _ = controller.Latency(250*time.Millisecond)
	if newConcurrency != 27 {
		t.Errorf("Expected concurrency to decrease to 27 (delta=2), got %d", newConcurrency)
	}

	// High latency again - delta doubles to 4
	newConcurrency, _ = controller.Latency(250*time.Millisecond)
	if newConcurrency != 23 {
		t.Errorf("Expected concurrency to decrease to 23 (delta=4), got %d", newConcurrency)
	}

	// Now low latency - increase by 1, delta should halve from 8 to 4
	newConcurrency, _ = controller.Latency(50*time.Millisecond)
	if newConcurrency != 24 {
		t.Errorf("Expected concurrency to increase to 24, got %d", newConcurrency)
	}

	// High latency again - should decrease by 4 (halved delta from 8 to 4)
	newConcurrency, _ = controller.Latency(250*time.Millisecond)
	if newConcurrency != 20 {
		t.Errorf("Expected concurrency to decrease to 20 (delta=4 after halving), got %d", newConcurrency)
	}

	// Middle latency - no change, delta should halve from 8 to 4
	newConcurrency, changed := controller.Latency(150*time.Millisecond)
	if newConcurrency != 20 {
		t.Errorf("Expected concurrency to remain at 20, got %d", newConcurrency)
	}
	if changed {
		t.Errorf("Expected no change for middle latency")
	}

	// High latency - should decrease by 4 (halved again)
	newConcurrency, _ = controller.Latency(250*time.Millisecond)
	if newConcurrency != 16 {
		t.Errorf("Expected concurrency to decrease to 16 (delta=4 after halving), got %d", newConcurrency)
	}
}