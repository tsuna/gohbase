// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// This file implements controller that are responsible for changing max number of
// concurrent operation for RegionClient.

package region

type Controller struct {
	lowThreshold   int
	highThreshold  int
	minConcurrency int
	maxConcurrency int

	currentConcurrency int
	decreaseDelta      int
}

// NewController creates a new concurrency controller with the specified thresholds and limits
func NewController(lowThreshold, highThreshold, minConcurrency, maxConcurrency int) *Controller {
	return &Controller{
		lowThreshold:       lowThreshold,
		highThreshold:      highThreshold,
		minConcurrency:     minConcurrency,
		maxConcurrency:     maxConcurrency,
		currentConcurrency: minConcurrency,
		decreaseDelta:      1,
	}
}

// Latency notifies controller about latest "ping" latency value. Return concurrency value.
func (cntr *Controller) Latency(val int) int {
	if val < cntr.lowThreshold {
		// Increase concurrency by 1
		cntr.currentConcurrency = min(cntr.currentConcurrency+1, cntr.maxConcurrency)
		// Halve decreaseDelta when increasing or stable
		cntr.decreaseDelta = max(cntr.decreaseDelta/2, 1)
	} else if val > cntr.highThreshold {
		// Decrease concurrency by decreaseDelta
		cntr.currentConcurrency = max(cntr.currentConcurrency-cntr.decreaseDelta, cntr.minConcurrency)
		// Double decreaseDelta when decreasing (only if less than maxConcurrency)
		if cntr.decreaseDelta < cntr.maxConcurrency {
			cntr.decreaseDelta = cntr.decreaseDelta * 2
		}
	} else {
		// Between thresholds - no change, halve decreaseDelta
		cntr.decreaseDelta = max(cntr.decreaseDelta/2, 1)
	}

	return cntr.currentConcurrency
}

// Concurrency return desired number of concurrent scans
func (cntr *Controller) Concurrency() int {
	return cntr.currentConcurrency
}
