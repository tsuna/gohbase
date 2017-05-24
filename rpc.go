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
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/zk"
)

// Constants
var (
	// Name of the meta region.
	metaTableName = []byte("hbase:meta")

	infoFamily = map[string][]string{
		"info": nil,
	}

	// ErrDeadline is returned when the deadline of a request has been exceeded
	ErrDeadline = errors.New("deadline exceeded")

	// ErrRegionUnavailable is returned when sending rpc to a region that is unavailable
	ErrRegionUnavailable = errors.New("region unavailable")

	// TableNotFound is returned when attempting to access a table that
	// doesn't exist on this cluster.
	TableNotFound = errors.New("table not found")

	// Default timeouts

	// How long to wait for a region lookup (either meta lookup or finding
	// meta in ZooKeeper).  Should be greater than or equal to the ZooKeeper
	// session timeout.
	regionLookupTimeout = 30 * time.Second

	backoffStart = 16 * time.Millisecond
)

func (c *client) SendRPC(rpc hrpc.Call) (proto.Message, error) {
	// Check the cache for a region that can handle this request
	var err error

	for {
		// block in case someone is updating regions.
		// for example someone is replacing a region with a new one,
		// we want to wait for that to finish so that we don't do
		// unnecessary region lookups in case that's our region.
		reg := c.getRegionFromCache(rpc.Table(), rpc.Key())
		if reg == nil {
			reg, err = c.findRegion(rpc.Context(), rpc.Table(), rpc.Key())
			if err == ErrRegionUnavailable {
				continue
			} else if err != nil {
				return nil, err
			}
		}

		msg, err := c.sendRPCToRegion(rpc, reg)
		switch err {
		case ErrRegionUnavailable:
			if ch := reg.AvailabilityChan(); ch != nil {
				// The region is unavailable. Wait for it to become available,
				// a new region or for the deadline to be exceeded.
				select {
				case <-rpc.Context().Done():
					return nil, ErrDeadline
				case <-ch:
				}
			}
		default:
			return msg, err
		}
	}
}

func sendBlocking(rc hrpc.RegionClient, rpc hrpc.Call) (hrpc.RPCResult, error) {
	rc.QueueRPC(rpc)

	var res hrpc.RPCResult
	// Wait for the response
	select {
	case res = <-rpc.ResultChan():
		return res, nil
	case <-rpc.Context().Done():
		return res, ErrDeadline
	}
}

func (c *client) sendRPCToRegion(rpc hrpc.Call, reg hrpc.RegionInfo) (proto.Message, error) {
	if reg.IsUnavailable() {
		return nil, ErrRegionUnavailable
	}
	rpc.SetRegion(reg)

	// Queue the RPC to be sent to the region
	client := reg.Client()
	if client == nil {
		// There was an error queueing the RPC.
		// Mark the region as unavailable.
		if reg.MarkUnavailable() {
			// If this was the first goroutine to mark the region as
			// unavailable, start a goroutine to reestablish a connection
			go c.reestablishRegion(reg)
		}
		return nil, ErrRegionUnavailable
	}
	res, err := sendBlocking(client, rpc)
	if err != nil {
		return nil, err
	}
	// Check for errors
	switch res.Error.(type) {
	case region.RetryableError:
		// There's an error specific to this region, but
		// our region client is fine. Mark this region as
		// unavailable (as opposed to all regions sharing
		// the client), and start a goroutine to reestablish
		// it.
		if reg.MarkUnavailable() {
			go c.reestablishRegion(reg)
		}
		return nil, ErrRegionUnavailable
	case region.UnrecoverableError:
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
			// Else this is a normal region. Mark all the regions
			// sharing this region's client as unavailable, and start
			// a goroutine to reconnect for each of them.
			downregions := c.clients.clientDown(client)
			for downreg := range downregions {
				if downreg.MarkUnavailable() {
					downreg.SetClient(nil)
					go c.reestablishRegion(downreg)
				}
			}
		}

		// Fall through to the case of the region being unavailable,
		// which will result in blocking until it's available again.
		return nil, ErrRegionUnavailable
	default:
		// RPC was successfully sent, or an unknown type of error
		// occurred. In either case, return the results.
		return res.Msg, res.Error
	}
}

func (c *client) lookupRegion(ctx context.Context,
	table, key []byte) (hrpc.RegionInfo, string, uint16, error) {
	var reg hrpc.RegionInfo
	var host string
	var port uint16
	var err error
	backoff := backoffStart
	for {
		// If it takes longer than regionLookupTimeout, fail so that we can sleep
		lookupCtx, cancel := context.WithTimeout(ctx, regionLookupTimeout)
		if c.clientType == adminClient {
			log.WithField("resource", zk.Master).Debug("looking up master")

			host, port, err = c.zkLookup(lookupCtx, zk.Master)
			cancel()
			reg = c.adminRegionInfo
		} else if bytes.Compare(table, metaTableName) == 0 {
			log.WithField("resource", zk.Meta).Debug("looking up region server of hbase:meta")

			host, port, err = c.zkLookup(lookupCtx, zk.Meta)
			cancel()
			reg = c.metaRegionInfo
		} else {
			log.WithFields(log.Fields{
				"table": string(table),
				"key":   string(key),
			}).Debug("looking up region")

			reg, host, port, err = c.metaLookup(lookupCtx, table, key)
			cancel()
			if err == TableNotFound {
				log.WithFields(log.Fields{
					"table": string(table),
					"key":   string(key),
					"err":   err,
				}).Debug("hbase:meta does not know about this table/key")
				return nil, "", 0, err
			}
		}
		if err == nil {
			log.WithFields(log.Fields{
				"table":  string(table),
				"key":    string(key),
				"region": reg,
				"host":   host,
				"port":   port,
			}).Debug("looked up a region")

			return reg, host, port, nil
		}
		log.WithFields(log.Fields{
			"table":   string(table),
			"key":     string(key),
			"backoff": backoff,
			"err":     err,
		}).Error("failed looking up region")
		// This will be hit if there was an error locating the region
		backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
		if err != nil {
			return nil, "", 0, err
		}
	}
}

func (c *client) findRegion(ctx context.Context, table, key []byte) (hrpc.RegionInfo, error) {
	// The region was not in the cache, it
	// must be looked up in the meta table
	reg, host, port, err := c.lookupRegion(ctx, table, key)
	if err != nil {
		return nil, err
	}

	reg.MarkUnavailable()
	if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
		// Check that the region wasn't added to
		// the cache while we were looking it up.
		overlaps, replaced := c.regions.put(reg)
		if !replaced {
			// the same or younger regions are already in cache,
			// iterate over overlaps to find the one that's right for our key
			for _, r := range overlaps {
				// overlaps are always the same table and in order,
				// just compare stop keys
				if bytes.Compare(key, r.StopKey()) < 0 || len(r.StopKey()) == 0 {
					return r, nil
				}
			}
			// our key is not in overlaps, this can happen in case there
			// was a split, but somehow we got a pre-split region
			// and splitA retion is already in cache and our key
			// is in splitB, so we need to retry.
			return nil, ErrRegionUnavailable
		}
		// otherwise, new region in cache, delete overlaps from client's cache
		for _, r := range overlaps {
			c.clients.del(r)
		}
	}

	// Start a goroutine to connect to the region
	go c.establishRegion(reg, host, port)

	// Wait for the new region to become
	// available, and then send the RPC
	return reg, nil
}

// Searches in the regions cache for the region hosting the given row.
func (c *client) getRegionFromCache(table, key []byte) hrpc.RegionInfo {
	if c.clientType == adminClient {
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
	metaKey := make([]byte, 0, len(table)+len(key)+3)
	metaKey = append(metaKey, table...)
	metaKey = append(metaKey, ',')
	metaKey = append(metaKey, key...)
	metaKey = append(metaKey, ',')
	// ':' is the first byte greater than '9'.  We always want to find the
	// entry with the greatest timestamp, so by looking right before ':'
	// we'll find it.
	metaKey = append(metaKey, ':')
	return metaKey
}

// metaLookup checks meta table for the region in which the given row key for the given table is.
func (c *client) metaLookup(ctx context.Context,
	table, key []byte) (hrpc.RegionInfo, string, uint16, error) {

	metaKey := createRegionSearchKey(table, key)
	rpc, err := hrpc.NewGetBefore(ctx, metaTableName, metaKey, hrpc.Families(infoFamily))
	if err != nil {
		return nil, "", 0, err
	}

	resp, err := c.Get(rpc)
	if err != nil {
		return nil, "", 0, err
	}
	if len(resp.Cells) == 0 {
		return nil, "", 0, TableNotFound
	}

	reg, host, port, err := region.ParseRegionInfo(resp)
	if err != nil {
		return nil, "", 0, err
	}
	if !bytes.Equal(table, fullyQualifiedTable(reg)) {
		// This would indicate a bug in HBase.
		return nil, "", 0, fmt.Errorf("wtf: meta returned an entry for the wrong table!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	} else if len(reg.StopKey()) != 0 &&
		bytes.Compare(key, reg.StopKey()) >= 0 {
		// This would indicate a hole in the meta table.
		return nil, "", 0, fmt.Errorf("wtf: meta returned an entry for the wrong region!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	}
	return reg, host, port, nil
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
	log.WithField("region", reg).Debug("reestablishing region")
	c.establishRegion(reg, "", 0)
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
func isRegionEstablished(rc hrpc.RegionClient, reg hrpc.RegionInfo) bool {
	probeGet, err := hrpc.NewGet(context.Background(), fullyQualifiedTable(reg), probeKey(reg))
	if err != nil {
		panic(fmt.Sprintf("should not happen: %s", err))
	}
	probeGet.ExistsOnly()

	probeGet.SetRegion(reg)
	resGet, err := sendBlocking(rc, probeGet)
	if err != nil {
		panic(fmt.Sprintf("should not happen: %s", err))
	}
	_, ok := resGet.Error.(region.RetryableError)
	return !ok
}

func (c *client) establishRegion(reg hrpc.RegionInfo, host string, port uint16) {
	var backoff time.Duration
	var err error
	for {
		backoff, err = sleepAndIncreaseBackoff(reg.Context(), backoff)
		if err != nil {
			// region is dead
			reg.MarkAvailable()
			return
		}
		if host == "" && port == 0 {
			// need to look up region and address of the regionserver
			originalReg := reg
			// lookup region forever until we get it or we learn that it doesn't exist
			reg, host, port, err = c.lookupRegion(reg.Context(),
				fullyQualifiedTable(originalReg),
				originalReg.StartKey())
			if err == TableNotFound {
				// region doesn't exist, delete it from caches
				c.regions.del(originalReg)
				c.clients.del(originalReg)
				originalReg.MarkAvailable()

				log.WithFields(log.Fields{
					"region":  originalReg.String(),
					"err":     err,
					"backoff": backoff,
				}).Info("region does not exist anymore")
				return
			} else if err == ErrDeadline {
				// region is dead
				originalReg.MarkAvailable()
				log.WithFields(log.Fields{
					"region":  originalReg.String(),
					"err":     err,
					"backoff": backoff,
				}).Info("region became dead while establishing client for it")
				return
			} else if err != nil {
				log.WithFields(log.Fields{
					"region":  originalReg.String(),
					"err":     err,
					"backoff": backoff,
				}).Fatal("unknown error occured when looking up region")
			}
			if !bytes.Equal(reg.Name(), originalReg.Name()) {
				// put new region and remove overlapping ones.
				// Should remove the original region as well.
				reg.MarkUnavailable()
				overlaps, replaced := c.regions.put(reg)
				if !replaced {
					// a region that is the same or younger is already in cache
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

		// connect to the region's regionserver
		if client, err := c.establishRegionClient(reg, host, port); err == nil {
			if c.clientType != adminClient {
				if existing := c.clients.put(client, reg); existing != client {
					// a client for this regionserver is already in cache, discard this one.
					client.Close()
					client = existing
				}
			}

			if isRegionEstablished(client, reg) {
				// set region client so that as soon as we mark it available,
				// concurrent readers are able to find the client
				reg.SetClient(client)
				reg.MarkAvailable()
				return
			}
		} else if err == context.Canceled {
			// region is dead
			reg.MarkAvailable()
			return
		}
		log.WithFields(log.Fields{
			"region":  reg,
			"backoff": backoff,
		}).Debug("region was not established, retrying")
		// reset address because we weren't able to connect to it
		// or regionserver says it's still offline, should look up again
		host, port = "", 0
	}
}

func sleepAndIncreaseBackoff(ctx context.Context, backoff time.Duration) (time.Duration, error) {
	if backoff == 0 {
		return backoffStart, nil
	}
	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return 0, ErrDeadline
	}
	// TODO: Revisit how we back off here.
	if backoff < 5000*time.Millisecond {
		return backoff * 2, nil
	} else {
		return backoff + 5000*time.Millisecond, nil
	}
}

func (c *client) establishRegionClient(reg hrpc.RegionInfo,
	host string, port uint16) (hrpc.RegionClient, error) {
	if c.clientType != adminClient {
		// if rpc is not for hbasemaster, check if client for regionserver
		// already exists
		if client := c.clients.checkForClient(host, port); client != nil {
			// There's already a client
			return client, nil
		}
	}

	var clientType region.ClientType
	if c.clientType == standardClient {
		clientType = region.RegionClient
	} else {
		clientType = region.MasterClient
	}
	clientCtx, cancel := context.WithTimeout(reg.Context(), regionLookupTimeout)
	defer cancel()
	return region.NewClient(clientCtx, host, port, clientType,
		c.rpcQueueSize, c.flushInterval, c.effectiveUser)
}

// zkResult contains the result of a ZooKeeper lookup (when we're looking for
// the meta region or the HMaster).
type zkResult struct {
	host string
	port uint16
	err  error
}

// zkLookup asynchronously looks up the meta region or HMaster in ZooKeeper.
func (c *client) zkLookup(ctx context.Context, resource zk.ResourceName) (string, uint16, error) {
	// We make this a buffered channel so that if we stop waiting due to a
	// timeout, we won't block the zkLookupSync() that we start in a
	// separate goroutine.
	reschan := make(chan zkResult, 1)
	go func() {
		host, port, err := c.zkClient.LocateResource(resource.Prepend(c.zkRoot))
		// This is guaranteed to never block as the channel is always buffered.
		reschan <- zkResult{host, port, err}
	}()
	select {
	case res := <-reschan:
		return res.host, res.port, res.err
	case <-ctx.Done():
		return "", 0, ErrDeadline
	}
}
