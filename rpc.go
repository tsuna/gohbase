// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/internal/observability"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/zk"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type regionInfoAndAddr struct {
	regionInfo hrpc.RegionInfo
	addr       string
}

// Constants
var (
	// Name of the meta region.
	metaTableName = []byte("hbase:meta")

	infoFamily = map[string][]string{
		"info": nil,
	}

	// TableNotFound is returned when attempting to access a table that
	// doesn't exist on this cluster.
	TableNotFound = errors.New("table not found")

	// ErrCannotFindRegion is returned when it took too many tries to find a
	// region for the request. It's likely that hbase:meta has overlaps or some other
	// inconsistency.
	ErrCannotFindRegion = errors.New("cannot find region for the rpc")

	// ErrClientClosed is returned when the gohbase client has been closed
	ErrClientClosed = errors.New("client is closed")
)

const (
	// maxFindRegionTries is the maximum number of times to try to send an RPC
	maxFindRegionTries = 10

	backoffStart = 16 * time.Millisecond
)

func (c *client) getRegionForRpc(ctx context.Context, rpc hrpc.Call) (hrpc.RegionInfo, error) {
	for i := 0; i < maxFindRegionTries; i++ {
		// Check the cache for a region that can handle this request
		if reg := c.getRegionFromCache(rpc.Table(), rpc.Key()); reg != nil {
			return reg, nil
		}

		if reg, err := c.findRegion(ctx, rpc.Table(), rpc.Key()); reg != nil {
			return reg, nil
		} else if err != nil {
			return nil, err
		}
	}
	return nil, ErrCannotFindRegion
}

func (c *client) SendRPC(rpc hrpc.Call) (msg proto.Message, err error) {
	start := time.Now()
	description := rpc.Description()
	ctx, sp := observability.StartSpan(rpc.Context(), description,
		trace.WithAttributes(
			attribute.String("table", strconv.Quote(string(rpc.Table()))),
			attribute.String("key", strconv.Quote(string(rpc.Key()))),
		))
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
			sp.SetStatus(codes.Error, err.Error())
		}

		o := operationDurationSeconds.WithLabelValues(description, result)

		observability.ObserveWithTrace(ctx, o, time.Since(start).Seconds())
		sp.End()
	}()

	backoff := backoffStart
	serverErrorCount := 0
	for {
		var rc hrpc.RegionClient
		rc, err = c.getRegionAndClientForRPC(ctx, rpc)
		if err != nil {
			return nil, err
		}
		msg, err = c.sendRPCToRegionClient(ctx, rpc, rc)
		switch err.(type) {
		case region.RetryableError:
			if scan, ok := rpc.(*hrpc.Scan); ok {
				c.scanRpcScanStats(scan, msg, err, true, start, time.Now())
			}
			sp.AddEvent("retrySleep")
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				return msg, err
			}
			continue // retry
		case region.ServerError:
			if scan, ok := rpc.(*hrpc.Scan); ok {
				c.scanRpcScanStats(scan, msg, err, true, start, time.Now())
			}
			// Retry ServerError immediately, as we want failover fast to
			// another server. But if HBase keep sending us ServerError, we
			// should start to backoff. We don't want to overwhelm HBase.
			if serverErrorCount > 1 {
				sp.AddEvent("retrySleep")
				backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
				if err != nil {
					return msg, err
				}
			}
			serverErrorCount++
			continue // retry
		case region.NotServingRegionError:
			if scan, ok := rpc.(*hrpc.Scan); ok {
				c.scanRpcScanStats(scan, msg, err, true, start, time.Now())
			}
			continue // retry
		}
		if scan, ok := rpc.(*hrpc.Scan); ok {
			c.scanRpcScanStats(scan, msg, err, false, start, time.Now())
		}
		return msg, err
	}
}

func (c *client) scanRpcScanStats(scan *hrpc.Scan, resp proto.Message, err error,
	retry bool, start, end time.Time) {
	if scan.ScanStatsHandler() != nil {
		stats := &hrpc.ScanStats{}
		// Update the ScanMetrics if they are being tracked. For ScanStats, these ScanMetrics
		// are collected per call to SendRPC and therefore may not be reflective of the entire
		// result of the Scan request if the results are split across multiple calls to
		// Scanner.Next().
		if scan.TrackScanMetrics() && resp != nil {
			scanres, ok := resp.(*pb.ScanResponse)
			if !ok {
				c.logger.Debug("got non ScanResponse for ScanRequest, no ScanMetrics to add")
			} else {
				if scanres.ScanMetrics != nil {
					stats.ScanMetrics = make(map[string]int64)
					for _, m := range scanres.ScanMetrics.GetMetrics() {
						stats.ScanMetrics[m.GetName()] = m.GetValue()
					}
				}
			}
		}

		stats.Table = scan.Table()
		stats.StartRow = scan.StartRow()
		stats.EndRow = scan.StopRow()
		if reg := scan.Region(); reg != nil {
			stats.RegionID = reg.ID()
			if cl := reg.Client(); cl != nil {
				stats.RegionServer = cl.Addr()
			}
		}
		stats.ScannerID = scan.ScannerId()
		stats.ScanStatsID = scan.ScanStatsID()
		stats.Start = start
		stats.End = end
		if err != nil {
			stats.Error = true
		}
		stats.Retryable = retry
		scan.ScanStatsHandler()(stats)
	}
}

func (c *client) getRegionAndClientForRPC(ctx context.Context, rpc hrpc.Call) (
	hrpc.RegionClient, error) {
	for {
		reg, err := c.getRegionForRpc(ctx, rpc)
		if err != nil {
			return nil, err
		}
		if ch := reg.AvailabilityChan(); ch != nil { // region is currently unavailable
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.done:
				return nil, ErrClientClosed
			case <-ch:
			}
		}

		client := reg.Client()
		if client == nil {
			// There was an error getting the region client. Mark the
			// region as unavailable.
			if reg.MarkUnavailable() {
				// If this was the first goroutine to mark the region as
				// unavailable, start a goroutine to reestablish a connection
				go c.reestablishRegion(reg)
			}
			if ch := reg.AvailabilityChan(); ch != nil {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-c.done:
					return nil, ErrClientClosed
				case <-ch:
				}
			}
			if reg.Context().Err() != nil {
				// region is dead because it was split or merged,
				// retry lookup
				continue
			}
			client = reg.Client()
			if client == nil {
				continue
			}
		}
		rpc.SetRegion(reg)
		return client, nil
	}
}

var (
	// NotExecutedError is returned when an RPC in a batch is not
	// executed due to encountering a different error in the batch.
	NotExecutedError = errors.New(
		"RPC in batch not executed due to another error")
)

// SendBatch will execute all the Calls in batch. Every Call must have
// the same table and must be Batchable.
//
// SendBatch will discover the correct region and region server for
// each Call and dispatch the Calls accordingly. SendBatch is not an
// atomic operation. Some calls may fail and others succeed. Calls
// sharing a region will execute in the order passed into SendBatch.
//
// SendBatch returns a slice of [hrpc.RPCResult] each containing a
// response and an error. The results will be returned in the same
// order as the Calls in the batch, in other words the i'th result
// will be for the i'th call. A nil error means the Call executed
// successfully. allOK is true if all calls completed successfully,
// and false if any calls failed and the errors in the results need to
// be checked.
//
// SendBatch will continue retrying each RPC in batch until it
// succeeds, fails with a non-retryable error, or the context is
// canceled.
func (c *client) SendBatch(ctx context.Context, batch []hrpc.Call) (
	res []hrpc.RPCResult, allOK bool) {
	if len(batch) == 0 {
		return nil, true
	}

	allOK = true

	start := time.Now()
	description := "SendBatch"
	ctx, sp := observability.StartSpan(ctx, description)
	defer func() {
		result := "ok"
		if !allOK {
			result = "error"
			sp.SetStatus(codes.Error, "batch error")
		}

		o := operationDurationSeconds.WithLabelValues(description, result)

		observability.ObserveWithTrace(ctx, o, time.Since(start).Seconds())
		sp.End()
	}()

	table := batch[0].Table()
	res = make([]hrpc.RPCResult, len(batch))
	rpcToRes := make(map[hrpc.Call]int, len(batch))
	for i, rpc := range batch {
		// map Call to index in res so that we can set the correct
		// result as Calls complete
		if j, dup := rpcToRes[rpc]; dup {
			res[i].Error = fmt.Errorf("duplicate call in batch at index %d", j)
			allOK = false
			continue
		}
		rpcToRes[rpc] = i

		// Initialize res with NotExecutedError. As RPCs are executed this
		// will be replaced by a more specific error or nil if no error
		// occurs.
		res[i].Error = NotExecutedError

		if !bytes.Equal(rpc.Table(), table) {
			res[i].Error = fmt.Errorf("multiple tables in batch request: %q and %q",
				string(table), string(rpc.Table()))
			allOK = false
		} else if !hrpc.CanBatch(rpc) {
			res[i].Error = errors.New("non-batchable call passed to SendBatch")
			allOK = false
		}
	}
	if !allOK {
		return res, allOK
	}

	// Send and wait for responses loop. This loop will partition the
	// batch per-regionserver batches, send those batches to the
	// region server and wait for results. Any RPCs that hit retryable
	// errors will be made into a new batch and passed through this
	// loop again.

	// unretryableErrorSeen set to true when any RPC in the batch hits
	// an error that is not retryable. This is used to remember to
	// return allOK=false even after we retry RPCs that hit retryable
	// errors and those all succeed.
	var unretryableErrorSeen bool
	var retries []hrpc.Call
	backoff := backoffStart

	for {
		rpcByClient, ok := c.findClients(ctx, batch, res)
		if !ok {
			return res, false
		}
		sendBatchSplitCount.Observe(float64(len(rpcByClient)))

		// Send each group of RPCs to region client to be executed.
		type clientAndRPCs struct {
			client hrpc.RegionClient
			rpcs   []hrpc.Call
		}
		// keep track of the order requests are queued so that we can wait
		// for their responses in the same order.
		cAndRs := make([]clientAndRPCs, 0, len(rpcByClient))
		for client, rpcs := range rpcByClient {
			client.QueueBatch(ctx, rpcs)
			cAndRs = append(cAndRs, clientAndRPCs{client, rpcs})
		}

		// batch wil be used to hold any RPCs that need to be retried
		batch = batch[:0]
		var needBackoff bool

		func() { // func used to scope the span
			ctx, sp := observability.StartSpan(ctx, "waitForResult")
			defer sp.End()
			for _, cAndR := range cAndRs {
				shouldRetry, shouldBackoff, unretryableError, ok := c.waitForCompletion(
					ctx, cAndR.client, cAndR.rpcs, res, rpcToRes)
				if !ok {
					allOK = false
					retries = append(retries, shouldRetry...)
					needBackoff = needBackoff || shouldBackoff
					unretryableErrorSeen = unretryableErrorSeen || unretryableError
				}
			}
		}()

		// Exit retry loop if no RPCs are retryable because they all
		// succeeded or hit unretryable errors (this is true if
		// retries is empty), or the context is done.
		if len(retries) == 0 || ctx.Err() != nil {
			break
		}
		if needBackoff {
			sp.AddEvent("retrySleep")
			var err error
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				break
			}
		} else {
			sp.AddEvent("retry")
		}
		// Set state for next loop iteration
		batch = retries
		retries = retries[:0]
		allOK = !unretryableErrorSeen
	}

	return res, allOK
}

// findClients takes a batch of rpcs and discovers the region and
// region client associated with each. A map is returned with rpcs
// grouped by their region client. If any error is encountered, the
// corresponding slot in res will be updated with that error and a
// BatchError is returned.
//
// findClients will not return on the first errror encountered. It
// will iterate through all the RPCs to ensure that all unknown
// regions encountered in the batch will start being initialized.
func (c *client) findClients(ctx context.Context, batch []hrpc.Call, res []hrpc.RPCResult) (
	map[hrpc.RegionClient][]hrpc.Call, bool) {

	rpcByClient := make(map[hrpc.RegionClient][]hrpc.Call)
	ok := true
	for i, rpc := range batch {
		rc, err := c.getRegionAndClientForRPC(ctx, rpc)
		if err != nil {
			res[i].Error = err
			ok = false
			continue // see if any more RPCs are missing regions
		}
		rpcByClient[rc] = append(rpcByClient[rc], rpc)
	}
	return rpcByClient, ok
}

// waitForCompletion waits for the completion of all rpcs, updating
// the appropriate index in results with the help of rpcToRes. If all
// rpcs succeed then ok will return true, otherwise:
//   - ok will be false
//   - retryables will contain RPCs that can be retried
//   - shouldBackoff will be true if any retryable RPCs need a backoff before retrying
//   - unretryableError will be true if there were errors seen on RPCs
//     that were not retryable. It communicates that retryables does
//     not contain all the RPCs that failed, so even though those
//     retryable RPCs may eventually succeed we need to return !ok to
//     the caller of SendBatch.
func (c *client) waitForCompletion(ctx context.Context, rc hrpc.RegionClient,
	rpcs []hrpc.Call, results []hrpc.RPCResult, rpcToRes map[hrpc.Call]int) (
	retryables []hrpc.Call, shouldBackoff, unretryableError, ok bool) {

	ok = true
	canceledIndex := len(rpcs)

loop:
	for i, rpc := range rpcs {
		select {
		case res := <-rpc.ResultChan():
			results[rpcToRes[rpc]] = res
			if res.Error != nil {
				c.handleResultError(res.Error, rpc.Region(), rc)
				ok = false
				switch res.Error.(type) {
				case region.RetryableError:
					shouldBackoff = true
					retryables = append(retryables, rpc)
				case region.ServerError, region.NotServingRegionError:
					retryables = append(retryables, rpc)
				default:
					unretryableError = true
				}
			}

		case <-ctx.Done():
			canceledIndex = i
			ok = false
			break loop
		}
	}

	// If the context was canceled we may have exited the loop above
	// without checking for every result. Do a non-blocking read of
	// the ResultChan for the remaining RPCs. If not ready the result
	// will be the context error.
	for _, rpc := range rpcs[canceledIndex:] {
		select {
		case res := <-rpc.ResultChan():
			results[rpcToRes[rpc]] = res
			if res.Error != nil {
				c.handleResultError(res.Error, rpc.Region(), rc)
			}
		default:
			results[rpcToRes[rpc]].Error = ctx.Err()
		}
	}

	return retryables, shouldBackoff, unretryableError, ok
}

// handleResultErrorOverride can be used to override behaviour of handleResultError in testing
var handleResultErrorOverride func(err error, reg hrpc.RegionInfo, rc hrpc.RegionClient)

func (c *client) handleResultError(err error, reg hrpc.RegionInfo, rc hrpc.RegionClient) {
	if handleResultErrorOverride != nil {
		handleResultErrorOverride(err, reg, rc)
		return
	}

	// Check for errors
	switch err.(type) {
	case region.NotServingRegionError:
		// There's an error specific to this region, but
		// our region client is fine. Mark this region as
		// unavailable (as opposed to all regions sharing
		// the client), and start a goroutine to reestablish
		// it.
		if reg.MarkUnavailable() {
			go c.reestablishRegion(reg)
		}
	case region.ServerError:
		// If it was an unrecoverable error, the region client is
		// considered dead.
		if reg == c.adminRegionInfo {
			// If this is the admin client, mark the region
			// as unavailable and start up a goroutine to
			// reconnect if it wasn't already marked as such.
			if reg.MarkUnavailable() {
				go c.reestablishRegion(reg)
			}
		} else {
			c.clientDown(rc, reg)
		}
	}
}

func sendBlocking(ctx context.Context, rc hrpc.RegionClient, rpc hrpc.Call) (
	hrpc.RPCResult, error) {
	rc.QueueRPC(rpc)

	ctx, sp := observability.StartSpan(ctx, "waitForResult")
	defer sp.End()
	var res hrpc.RPCResult
	// Wait for the response
	select {
	case res = <-rpc.ResultChan():
		return res, nil
	case <-ctx.Done():
		return res, rpc.Context().Err()
	}
}

func (c *client) sendRPCToRegionClient(ctx context.Context, rpc hrpc.Call, rc hrpc.RegionClient) (
	proto.Message, error) {
	res, err := sendBlocking(ctx, rc, rpc)
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		c.handleResultError(res.Error, rpc.Region(), rc)
	}
	return res.Msg, res.Error
}

// clientDown removes client from cache and marks all the regions
// sharing this region's client as unavailable, and start a goroutine
// to reconnect for each of them.
//
// Due to races filling in the clients cache it may not be completely
// accurate. reg is the region we were trying to access when we saw an
// issue with the region client, so make sure it is marked unavailable
// even if it doesn't appear in the clients cache.
func (c *client) clientDown(client hrpc.RegionClient, reg hrpc.RegionInfo) {
	downregions := c.clients.clientDown(client)
	if reg.MarkUnavailable() {
		reg.SetClient(nil)
		go c.reestablishRegion(reg)
	}
	for downreg := range downregions {
		if downreg == reg {
			continue
		}
		if downreg.MarkUnavailable() {
			downreg.SetClient(nil)
			go c.reestablishRegion(downreg)
		}
	}
}

func (c *client) lookupRegion(ctx context.Context,
	table, key []byte) (hrpc.RegionInfo, string, error) {
	var reg hrpc.RegionInfo
	var addr string
	var err error
	backoff := backoffStart
	for {
		// If it takes longer than regionLookupTimeout, fail so that we can sleep
		lookupCtx, cancel := context.WithTimeout(ctx, c.regionLookupTimeout)
		if c.clientType == region.MasterClient {
			c.logger.Debug("looking up master", "resource", zk.Master)

			addr, err = c.zkLookup(lookupCtx, zk.Master)
			cancel()
			reg = c.adminRegionInfo
		} else if bytes.Equal(table, metaTableName) {
			c.logger.Debug("looking up region server of hbase:meta", "resource", zk.Meta)

			addr, err = c.zkLookup(lookupCtx, zk.Meta)
			cancel()
			reg = c.metaRegionInfo
		} else {
			c.logger.Debug("looking up region",
				"table", strconv.Quote(string(table)), "key", strconv.Quote(string(key)))

			reg, addr, err = c.metaLookup(lookupCtx, table, key)
			cancel()
			if err == TableNotFound {
				c.logger.Debug("hbase:meta does not know about this table/key",
					"table", strconv.Quote(string(table)),
					"key", strconv.Quote(string(key)), "err", err)

				return nil, "", err
			} else if err == ErrClientClosed {
				return nil, "", err
			}
		}
		if err == nil {
			c.logger.Debug("looked up a region", "table", strconv.Quote(string(table)),
				"key", strconv.Quote(string(key)), "region", reg, "addr", addr)

			return reg, addr, nil
		}

		c.logger.Error("failed looking up region", "table", strconv.Quote(string(table)),
			"key", strconv.Quote(string(key)), "backoff", backoff, "err", err)

		// This will be hit if there was an error locating the region
		backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
		if err != nil {
			return nil, "", err
		}
	}
}

func (c *client) lookupAllRegions(ctx context.Context,
	table []byte) ([]regionInfoAndAddr, error) {
	var regs []regionInfoAndAddr
	var err error
	backoff := backoffStart
	for {
		// If it takes longer than regionLookupTimeout, fail so that we can sleep
		lookupCtx, cancel := context.WithTimeout(ctx, c.regionLookupTimeout)
		c.logger.Debug("looking up regions", "table", strconv.Quote(string(table)))

		regs, err = c.metaLookupForTable(lookupCtx, table)
		cancel()
		if err == TableNotFound {
			c.logger.Debug("hbase:meta does not know about this table",
				"table", strconv.Quote(string(table)), "err", err)

			return nil, err
		} else if err == ErrClientClosed {
			return nil, err
		}

		if err == nil {
			c.logger.Debug("looked up all regions",
				"table", strconv.Quote(string(table)), "regionsAndAddr", regs)

			return regs, nil
		}

		c.logger.Error("failed looking up regions", "table", strconv.Quote(string(table)),
			"backoff", backoff, "err", err)

		// This will be hit if there was an error locating the region
		backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
		if err != nil {
			return nil, err
		}
	}
}

func (c *client) findAllRegions(ctx context.Context, table []byte) ([]regionInfoAndAddr, error) {
	regs, err := c.lookupAllRegions(ctx, table)
	if err != nil {
		return nil, err
	}
	for _, regaddr := range regs {
		reg, addr := regaddr.regionInfo, regaddr.addr
		reg.MarkUnavailable()

		if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
			// Check that the region wasn't added to
			// the cache while we were looking it up.
			overlaps, replaced := c.regions.put(reg)
			if !replaced {
				// the same or younger regions are already in cache
				continue
			}

			// otherwise, new region in cache, delete overlaps from client's cache
			for _, r := range overlaps {
				c.clients.del(r)
			}
		}

		// Start a goroutine to connect to the region
		go c.establishRegion(reg, addr)
	}

	return regs, nil
}

func (c *client) findRegion(ctx context.Context, table, key []byte) (hrpc.RegionInfo, error) {
	// The region was not in the cache, it
	// must be looked up in the meta table
	reg, addr, err := c.lookupRegion(ctx, table, key)
	if err != nil {
		return nil, err
	}

	// We are the ones that looked up the region, so we need to
	// mark it unavailable and find a client for it.
	reg.MarkUnavailable()

	if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
		// Check that the region wasn't added to
		// the cache while we were looking it up.
		overlaps, replaced := c.regions.put(reg)
		if !replaced {
			// the same or younger regions are already in cache, retry looking up in cache
			return nil, nil
		}

		// otherwise, new region in cache, delete overlaps from client's cache
		for _, r := range overlaps {
			c.clients.del(r)
		}
	}

	// Start a goroutine to connect to the region
	go c.establishRegion(reg, addr)

	// Wait for the new region to become
	// available, and then send the RPC
	return reg, nil
}

// Searches in the regions cache for the region hosting the given row.
func (c *client) getRegionFromCache(table, key []byte) hrpc.RegionInfo {
	if c.clientType == region.MasterClient {
		return c.adminRegionInfo
	} else if bytes.Equal(table, metaTableName) {
		return c.metaRegionInfo
	}
	regionName := createRegionSearchKey(table, key)
	_, region := c.regions.get(regionName)
	if region == nil {
		return nil
	}

	// make sure the returned region is for the same table
	if !bytes.Equal(fullyQualifiedTable(region), table) {
		// not the same table, can happen if we got the last region
		return nil
	}

	if len(region.StopKey()) != 0 &&
		// If the stop key is an empty byte array, it means this region is the
		// last region for this table and this key ought to be in that region.
		bytes.Compare(key, region.StopKey()) >= 0 {
		return nil
	}

	return region
}

// Creates the META key to search for in order to locate the given key.
func createRegionSearchKey(table, key []byte) []byte {
	// Shorten the key such that the generated meta key is <= MAX_ROW_LENGTH (MaxInt16), otherwise
	// HBase will throw an exception.
	keylen := math.MaxInt16 - len(table) - 3
	if len(key) < keylen {
		keylen = len(key)
	}

	metaKey := make([]byte, 0, len(table)+keylen+3)
	metaKey = append(metaKey, table...)
	metaKey = append(metaKey, ',')
	metaKey = append(metaKey, key[:keylen]...)
	metaKey = append(metaKey, ',')
	// ':' is the first byte greater than '9'.  We always want to find the
	// entry with the greatest timestamp, so by looking right before ':'
	// we'll find it.
	metaKey = append(metaKey, ':')
	return metaKey
}

// metaLookup checks meta table for the region in which the given row key for the given table is.
func (c *client) metaLookup(ctx context.Context,
	table, key []byte) (hrpc.RegionInfo, string, error) {
	metaKey := createRegionSearchKey(table, key)
	rpc, err := hrpc.NewScanRange(ctx, metaTableName, metaKey, table,
		hrpc.Families(infoFamily),
		hrpc.Reversed(),
		hrpc.CloseScanner(),
		hrpc.NumberOfRows(1))
	if err != nil {
		return nil, "", err
	}

	scanner := c.Scan(rpc)
	resp, err := scanner.Next()
	if err == io.EOF {
		return nil, "", TableNotFound
	}
	if err != nil {
		return nil, "", err
	}

	reg, addr, err := region.ParseRegionInfo(resp)
	if err != nil {
		return nil, "", err
	}
	if !bytes.Equal(table, fullyQualifiedTable(reg)) {
		// This would indicate a bug in HBase.
		return nil, "", fmt.Errorf("meta returned an entry for the wrong table!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	} else if len(reg.StopKey()) != 0 &&
		bytes.Compare(key, reg.StopKey()) >= 0 {
		// This would indicate a hole in the meta table.
		return nil, "", fmt.Errorf("meta returned an entry for the wrong region!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	}
	return reg, addr, nil
}

// Creates the META key to search for all regions
func createAllRegionSearchKey(table []byte) []byte {
	metaKey := make([]byte, 0, len(table)+1)
	metaKey = append(metaKey, table...)
	// '.' is the first byte greater than ','.  Meta table entry has
	// the format table,key,timestamp. By adding '.' to the stop row
	// we scan all keys for table
	metaKey = append(metaKey, '.')
	return metaKey
}

// metaLookupForTable checks meta table for all the open table regions.
func (c *client) metaLookupForTable(ctx context.Context,
	table []byte) ([]regionInfoAndAddr, error) {
	metaKey := createAllRegionSearchKey(table)
	rpc, err := hrpc.NewScanRange(ctx, metaTableName, table, metaKey,
		hrpc.Families(infoFamily))
	if err != nil {
		return nil, err
	}

	var regions []regionInfoAndAddr
	scanner := c.Scan(rpc)
	for {
		resp, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		reg, addr, err := region.ParseRegionInfo(resp)
		if err != nil {
			// Ignore error, but log if it's anything else than OfflineRegionError. This really
			// shouldn't happen unless HBase meta table is corrupted/changed format.
			if _, ok := err.(region.OfflineRegionError); !ok {
				c.logger.Debug("failed to parse region", "err", err)
			}
			continue
		}

		regions = append(regions, regionInfoAndAddr{regionInfo: reg, addr: addr})
	}

	if len(regions) == 0 {
		return nil, TableNotFound
	}
	return regions, nil
}

func fullyQualifiedTable(reg hrpc.RegionInfo) []byte {
	namespace := reg.Namespace()
	table := reg.Table()
	if namespace == nil {
		return table
	}
	// non-default namespace table
	fqTable := make([]byte, 0, len(namespace)+1+len(table))
	fqTable = append(fqTable, namespace...)
	fqTable = append(fqTable, byte(':'))
	fqTable = append(fqTable, table...)
	return fqTable
}

func (c *client) reestablishRegion(reg hrpc.RegionInfo) {
	select {
	case <-c.done:
		return
	default:
	}

	c.logger.Debug("reestablishing region", "region", reg)
	c.establishRegion(reg, "")
}

// probeKey returns a key in region that is unlikely to have data at it
// in order to test if the region is online. This prevents the Get request
// to actually fetch the data from the storage which consumes resources
// of the region server
func probeKey(reg hrpc.RegionInfo) []byte {
	// now we create a probe key: reg.StartKey() + 17 zeros
	probe := make([]byte, len(reg.StartKey())+17)
	copy(probe, reg.StartKey())
	return probe
}

// isRegionEstablished checks whether regionserver accepts rpcs for the region.
// Returns the cause if not established.
func isRegionEstablished(rc hrpc.RegionClient, reg hrpc.RegionInfo) error {
	probe, err := hrpc.NewGet(context.Background(), fullyQualifiedTable(reg), probeKey(reg),
		hrpc.SkipBatch())
	if err != nil {
		panic(fmt.Sprintf("should not happen: %s", err))
	}
	probe.ExistsOnly()

	probe.SetRegion(reg)
	res, err := sendBlocking(probe.Context(), rc, probe)
	if err != nil {
		panic(fmt.Sprintf("should not happen: %s", err))
	}

	switch res.Error.(type) {
	case region.ServerError, region.NotServingRegionError, region.RetryableError:
		return res.Error
	default:
		return nil
	}
}

// establishRegionOverride can be set by tests to override the
// behavior of establishRegion
var establishRegionOverride func(reg hrpc.RegionInfo, addr string)

func (c *client) establishRegion(reg hrpc.RegionInfo, addr string) {
	if establishRegionOverride != nil {
		establishRegionOverride(reg, addr)
		return
	}

	var backoff time.Duration
	var err error
	for {
		backoff, err = sleepAndIncreaseBackoff(reg.Context(), backoff)
		if err != nil {
			// region is dead
			reg.MarkAvailable()
			return
		}
		if addr == "" {
			// need to look up region and address of the regionserver
			originalReg := reg
			// lookup region forever until we get it or we learn that it doesn't exist
			reg, addr, err = c.lookupRegion(originalReg.Context(),
				fullyQualifiedTable(originalReg), originalReg.StartKey())

			if err == TableNotFound {
				// region doesn't exist, delete it from caches
				c.regions.del(originalReg)
				c.clients.del(originalReg)
				originalReg.MarkAvailable()

				c.logger.Info("region does not exist anymore",
					"region", originalReg.String(), "err", err, "backoff", backoff)

				return
			} else if originalReg.Context().Err() != nil {
				// region is dead
				originalReg.MarkAvailable()

				c.logger.Info("region became dead while establishing client for it",
					"region", originalReg.String(), "err", err, "backoff", backoff)

				return
			} else if err == ErrClientClosed {
				// client has been closed
				return
			} else if err != nil {
				panic(fmt.Errorf("unknown error occurred when looking up region %q, "+
					"backoff=%s: %s", originalReg.String(), backoff, err))
			}
			if !bytes.Equal(reg.Name(), originalReg.Name()) {
				// put new region and remove overlapping ones.
				// Should remove the original region as well.
				reg.MarkUnavailable()
				overlaps, replaced := c.regions.put(reg)
				if !replaced {
					// a region that is the same or younger is already in cache
					reg.MarkAvailable()
					originalReg.MarkAvailable()
					return
				}
				// otherwise delete the overlapped regions in cache
				for _, r := range overlaps {
					c.clients.del(r)
				}
				// let rpcs know that they can retry and either get the newly
				// added region from cache or lookup the one they need
				originalReg.MarkAvailable()
			} else {
				// same region, discard the looked up one
				reg = originalReg
			}
		}

		var client hrpc.RegionClient
		if reg == c.adminRegionInfo {
			// admin region is used for talking to master, so it only has one connection to
			// master that we don't add to the cache
			// TODO: consider combining this case with the regular regionserver path
			client = c.newRegionClientFn(addr, c.clientType, c.rpcQueueSize, c.flushInterval,
				c.effectiveUser, c.regionReadTimeout, nil, c.regionDialer, c.logger)
		} else {
			client = c.clients.put(addr, reg, func() hrpc.RegionClient {
				return c.newRegionClientFn(addr, c.clientType, c.rpcQueueSize, c.flushInterval,
					c.effectiveUser, c.regionReadTimeout, c.compressionCodec,
					c.regionDialer, c.logger)
			})
		}

		// connect to the region's regionserver.
		// only the first caller to Dial gets to actually connect, other concurrent calls
		// will block until connected or an error.
		dialCtx, cancel := context.WithTimeout(reg.Context(), c.regionLookupTimeout)
		err = client.Dial(dialCtx)
		cancel()

		if err == nil {
			if reg == c.adminRegionInfo {
				reg.SetClient(client)
				reg.MarkAvailable()
				return
			}

			if err = isRegionEstablished(client, reg); err == nil {
				// set region client so that as soon as we mark it available,
				// concurrent readers are able to find the client
				reg.SetClient(client)
				reg.MarkAvailable()
				return
			} else if _, ok := err.(region.ServerError); ok {
				// the client we got died
				c.clientDown(client, reg)
			}
		} else if err == context.Canceled {
			// region is dead
			reg.MarkAvailable()
			return
		} else {
			// otherwise Dial failed, purge the client and retry.
			// note that it's safer to reestablish all regions for this client as well
			// because they could have ended up setteling for the same client.
			c.clientDown(client, reg)
		}

		c.logger.Debug("region was not established, retrying",
			"region", reg, "backoff", backoff, "err", err)
		// reset address because we weren't able to connect to it
		// or regionserver says it's still offline, should look up again
		addr = ""
	}
}

// sleepAndIncreaseBackoffOverride can be set by tests to override the
// behavior of sleepAndIncreaseBackoff
var sleepAndIncreaseBackoffOverride func(
	ctx context.Context, backoff time.Duration) (time.Duration, error)

func sleepAndIncreaseBackoff(ctx context.Context, backoff time.Duration) (time.Duration, error) {
	if sleepAndIncreaseBackoffOverride != nil {
		return sleepAndIncreaseBackoffOverride(ctx, backoff)
	}
	if backoff == 0 {
		return backoffStart, nil
	}

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	// Keep track of the amount of time spend sleeping in retry backoff. Ignore if context was
	// canceled.
	retryBackoffDuration.Observe(backoff.Seconds())

	// When changing this formula, update the buckets of the retryBackoffDuration metric too.
	if backoff < 5*time.Second {
		return backoff * 2, nil
	} else if backoff < 30*time.Second {
		return backoff + 5*time.Second, nil
	}
	return backoff, nil
}

// zkResult contains the result of a ZooKeeper lookup (when we're looking for
// the meta region or the HMaster).
type zkResult struct {
	addr string
	err  error
}

// zkLookup asynchronously looks up the meta region or HMaster in ZooKeeper.
func (c *client) zkLookup(ctx context.Context, resource zk.ResourceName) (string, error) {
	// We make this a buffered channel so that if we stop waiting due to a
	// timeout, we won't block the zkLookupSync() that we start in a
	// separate goroutine.
	reschan := make(chan zkResult, 1)
	go func() {
		addr, err := c.zkClient.LocateResource(resource.Prepend(c.zkRoot))
		// This is guaranteed to never block as the channel is always buffered.
		reschan <- zkResult{addr, err}
	}()
	select {
	case res := <-reschan:
		return res.addr, res.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
