// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// This file implements controller that are responsible for changing max number of
// concurrent operation for RegionClient.

package region

import "time"

type Controller struct {
	lowThreshold  time.Duration
	highThreshold time.Duration
	minVal        int
	maxVal        int

	current       int
	decreaseDelta int
}

// NewController creates a new controller with the specified thresholds and limits
func NewController(minVal, maxVal int, low, high time.Duration) *Controller {
	return &Controller{
		lowThreshold:  low,
		highThreshold: high,
		minVal:        minVal,
		maxVal:        maxVal,
		current:       minVal, // starting from lowest value
		decreaseDelta: 1,
	}
}

// Latency notifies controller about latest "ping" latency value. Return new value and
// true if value changed.
func (cntr *Controller) Latency(val time.Duration) (int, bool) {
	old := cntr.current
	if val < cntr.lowThreshold {
		// Increase by 1
		cntr.current = min(cntr.current+1, cntr.maxVal)
		// Halve decreaseDelta when increasing or stable
		cntr.decreaseDelta = max(cntr.decreaseDelta/2, 1)
	} else if val > cntr.highThreshold {
		// Decrease by decreaseDelta
		cntr.current = max(cntr.current-cntr.decreaseDelta, cntr.minVal)
		// Double decreaseDelta when decreasing (only if less than maxVal)
		if cntr.decreaseDelta < cntr.maxVal {
			cntr.decreaseDelta = cntr.decreaseDelta * 2
		}
	} else {
		// Between thresholds - no change, halve decreaseDelta
		cntr.decreaseDelta = max(cntr.decreaseDelta/2, 1)
	}

	return cntr.current, old != cntr.current
}

// Concurrency return desired number of concurrent scans
func (cntr *Controller) Concurrency() int {
	return cntr.current
}
