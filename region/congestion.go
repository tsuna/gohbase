// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// This file implements controller that are responsible for changing number of
// concurrent operation for RegionClient.

package region

import (
	"fmt"
	"time"
)

// NewControllerFunc is a factory function that creates a new Controller instance
// with the specified min and max window bounds.
type NewControllerFunc func(minWindow, maxWindow int) (Controller, error)

// Controller defines the interface for congestion control algorithms
type Controller interface {
	// Latency notifies controller about latest "ping" latency value.
	// Returns new window value and true if value changed.
	Latency(val time.Duration) (int, bool)
	// Window returns the current desired window (concurrency level)
	Window() int
}

// AIMDController implements Additive Increase Multiplicative Decrease congestion control
type AIMDController struct {
	lowThreshold  time.Duration
	highThreshold time.Duration
	minWindow     int
	maxWindow     int

	current       int
	decreaseDelta int
}

// NewAIMDController creates a new AIMD controller with the specified thresholds.
// This returns a factory function that creates controllers with specific window bounds.
func NewAIMDController(low, high time.Duration) NewControllerFunc {
	return func(minWindow, maxWindow int) (Controller, error) {
		if minWindow <= 0 {
			return nil, fmt.Errorf("minimum window must be greater than 0, got %d", minWindow)
		}

		if maxWindow <= 0 {
			return nil, fmt.Errorf("maximum window must be greater than 0, got %d", maxWindow)
		}

		if maxWindow < minWindow {
			return nil,
				fmt.Errorf("maximum window (%d) must be greater or equal to minimum window (%d)",
					maxWindow, minWindow)
		}
		return &AIMDController{
			lowThreshold:  low,
			highThreshold: high,
			minWindow:     minWindow,
			maxWindow:     maxWindow,
			current:       minWindow, // start at minimum window
			decreaseDelta: 1,
		}, nil
	}
}

// Latency notifies controller about latest "ping" latency value. Return new value and
// true if value changed.
func (cntr *AIMDController) Latency(val time.Duration) (int, bool) {
	old := cntr.current
	if val < cntr.lowThreshold {
		// Increase by 1
		cntr.current = min(cntr.current+1, cntr.maxWindow)
		// Halve decreaseDelta when increasing or stable
		cntr.decreaseDelta = max(cntr.decreaseDelta/2, 1)
	} else if val > cntr.highThreshold {
		// Decrease by decreaseDelta
		cntr.current = max(cntr.current-cntr.decreaseDelta, cntr.minWindow)
		// Double decreaseDelta when decreasing (only if less than maxWindow)
		if cntr.decreaseDelta < cntr.maxWindow {
			cntr.decreaseDelta = cntr.decreaseDelta * 2
		}
	} else {
		// Between thresholds - no change, halve decreaseDelta
		cntr.decreaseDelta = max(cntr.decreaseDelta/2, 1)
	}

	return cntr.current, old != cntr.current
}

// Window returns desired number of concurrent scans
func (cntr *AIMDController) Window() int {
	return cntr.current
}
