// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cznic/b"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/zk"
	"golang.org/x/net/context"
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

const (
	standardClient = iota
	adminClient
)

type Option func(*client)

// client -> region cache. Used to quickly look up all the
// regioninfos that map to a specific client
type clientRegionCache struct {
	m sync.Mutex

	regions map[hrpc.RegionClient][]hrpc.RegionInfo
}

func (rcc *clientRegionCache) put(c hrpc.RegionClient, r hrpc.RegionInfo) {
	rcc.m.Lock()
	defer rcc.m.Unlock()

	lst := rcc.regions[c]
	for _, existing := range lst {
		if existing == r {
			return
		}
	}
	rcc.regions[c] = append(lst, r)
}

func (rcc *clientRegionCache) del(r hrpc.RegionInfo) {
	rcc.m.Lock()
	c := r.Client()
	if c != nil {
		r.SetClient(nil)

		rs := rcc.regions[c]
		var index int
		for i, reg := range rs {
			if reg == r {
				index = i
			}
		}
		rs = append(rs[:index], rs[index+1:]...)

		if len(rs) == 0 {
			// close region client if noone is using it
			delete(rcc.regions, c)
			c.Close()
		} else {
			rcc.regions[c] = rs
		}
	}
	rcc.m.Unlock()
}

func (rcc *clientRegionCache) closeAll() {
	rcc.m.Lock()
	for client, regions := range rcc.regions {
		for _, region := range regions {
			region.MarkUnavailable()
			region.SetClient(nil)
		}
		client.Close()
	}
	rcc.m.Unlock()
}

func (rcc *clientRegionCache) clientDown(reg hrpc.RegionInfo) []hrpc.RegionInfo {
	rcc.m.Lock()
	var downregions []hrpc.RegionInfo
	c := reg.Client()
	for _, sharedReg := range rcc.regions[c] {
		succ := sharedReg.MarkUnavailable()
		sharedReg.SetClient(nil)
		if succ {
			downregions = append(downregions, sharedReg)
		}
	}
	delete(rcc.regions, c)
	rcc.m.Unlock()
	return downregions
}

func (rcc *clientRegionCache) checkForClient(host string, port uint16) hrpc.RegionClient {
	rcc.m.Lock()
	defer rcc.m.Unlock()

	for client := range rcc.regions {
		if client.Host() == host && client.Port() == port {
			return client
		}
	}
	return nil
}

// key -> region cache.
type keyRegionCache struct {
	m sync.Mutex

	// Maps a []byte of a region start key to a hrpc.RegionInfo
	regions *b.Tree
}

func (krc *keyRegionCache) get(key []byte) ([]byte, hrpc.RegionInfo) {
	// When seeking - "The Enumerator's position is possibly after the last item in the tree"
	// http://godoc.org/github.com/cznic/b#Tree.Set
	krc.m.Lock()

	enum, ok := krc.regions.Seek(key)
	k, v, err := enum.Prev()
	if err == io.EOF && krc.regions.Len() > 0 {
		// We're past the end of the tree. Return the last element instead.
		// (Without this code we always get a cache miss and create a new client for each req.)
		k, v = krc.regions.Last()
		err = nil
	} else if !ok {
		k, v, err = enum.Prev()
	}
	enum.Close()
	if err != nil {
		krc.m.Unlock()
		return nil, nil
	}
	krc.m.Unlock()
	return k.([]byte), v.(hrpc.RegionInfo)
}

func isRegionOverlap(regA, regB hrpc.RegionInfo) bool {
	return bytes.Equal(regA.Table(), regB.Table()) &&
		bytes.Compare(regA.StartKey(), regB.StopKey()) < 0 &&
		bytes.Compare(regA.StopKey(), regB.StartKey()) > 0
}

func (krc *keyRegionCache) getOverlaps(reg hrpc.RegionInfo) []hrpc.RegionInfo {
	var overlaps []hrpc.RegionInfo
	var v interface{}
	var err error

	// deal with empty tree in the beginning so that we don't have to check
	// EOF errors for enum later
	if krc.regions.Len() == 0 {
		return overlaps
	}

	enum, ok := krc.regions.Seek(reg.Name())
	if !ok {
		// need to check if there are overlaps before what we found
		_, _, err = enum.Prev()
		if err == io.EOF {
			// we are in the end of tree, get last entry
			_, v = krc.regions.Last()
			currReg := v.(hrpc.RegionInfo)
			if isRegionOverlap(currReg, reg) {
				return append(overlaps, currReg)
			}
		} else {
			_, v, err = enum.Next()
			if err == io.EOF {
				// we are before the beginning of the tree now, get new enum
				enum.Close()
				enum, err = krc.regions.SeekFirst()
			} else {
				// otherwise, check for overlap before us
				currReg := v.(hrpc.RegionInfo)
				if isRegionOverlap(currReg, reg) {
					overlaps = append(overlaps, currReg)
				}
			}
		}
	}

	// now append all regions that overlap until the end of the tree
	// or until they don't overlap
	_, v, err = enum.Next()
	for err == nil && isRegionOverlap(v.(hrpc.RegionInfo), reg) {
		overlaps = append(overlaps, v.(hrpc.RegionInfo))
		_, v, err = enum.Next()
	}
	enum.Close()
	return overlaps
}

func (krc *keyRegionCache) put(reg hrpc.RegionInfo) []hrpc.RegionInfo {
	krc.m.Lock()
	defer krc.m.Unlock()

	// Remove all the entries that are overlap with the range of the new region.
	os := krc.getOverlaps(reg)
	for _, o := range os {
		krc.regions.Delete(o.Name())
	}

	krc.regions.Put(reg.Name(), func(interface{}, bool) (interface{}, bool) {
		return reg, true
	})
	return os
}

func (krc *keyRegionCache) del(key []byte) bool {
	krc.m.Lock()
	success := krc.regions.Delete(key)
	krc.m.Unlock()
	return success
}

// A Client provides access to an HBase cluster.
type client struct {
	clientType int

	zkquorum string

	regions keyRegionCache

	// TODO: document what this protects.
	regionsLock sync.Mutex

	// Maps a hrpc.RegionInfo to the *region.Client that we think currently
	// serves it.
	clients clientRegionCache

	metaRegionInfo hrpc.RegionInfo

	adminRegionInfo hrpc.RegionInfo

	// The maximum size of the RPC queue in the region client
	rpcQueueSize int

	// zkClient is zookeeper for retrieving meta and admin information
	zkClient zk.Client

	// The timeout before flushing the RPC queue in the region client
	flushInterval time.Duration
}

// Client a regular HBase client
type Client interface {
	Scan(s *hrpc.Scan) ([]*hrpc.Result, error)
	Get(g *hrpc.Get) (*hrpc.Result, error)
	Put(p *hrpc.Mutate) (*hrpc.Result, error)
	Delete(d *hrpc.Mutate) (*hrpc.Result, error)
	Append(a *hrpc.Mutate) (*hrpc.Result, error)
	Increment(i *hrpc.Mutate) (int64, error)
	CheckAndPut(p *hrpc.Mutate, family string, qualifier string,
		expectedValue []byte) (bool, error)
	Close()
}

// AdminClient to perform admistrative operations with HMaster
type AdminClient interface {
	CreateTable(t *hrpc.CreateTable) error
	DeleteTable(t *hrpc.DeleteTable) error
	EnableTable(t *hrpc.EnableTable) error
	DisableTable(t *hrpc.DisableTable) error
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string, options ...Option) Client {
	return newClient(zkquorum, options...)
}

// NewAdminClient creates an admin HBase client.
func NewAdminClient(zkquorum string, options ...Option) AdminClient {
	c := newClient(zkquorum, options...)
	c.clientType = adminClient
	return c
}

func newClient(zkquorum string, options ...Option) *client {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new client.")
	c := &client{
		clientType: standardClient,
		regions:    keyRegionCache{regions: b.TreeNew(region.CompareGeneric)},
		clients: clientRegionCache{
			regions: make(map[hrpc.RegionClient][]hrpc.RegionInfo),
		},
		zkquorum:      zkquorum,
		rpcQueueSize:  100,
		flushInterval: 20 * time.Millisecond,
		metaRegionInfo: region.NewInfo(
			[]byte("hbase:meta"),
			[]byte("hbase:meta,,1"),
			nil,
			nil),
		adminRegionInfo: region.NewInfo(
			nil,
			nil,
			nil,
			nil),
		zkClient: zk.NewClient(zkquorum),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// RpcQueueSize will return an option that will set the size of the RPC queues
// used in a given client
func RpcQueueSize(size int) Option {
	return func(c *client) {
		c.rpcQueueSize = size
	}
}

// FlushInterval will return an option that will set the timeout for flushing
// the RPC queues used in a given client
func FlushInterval(interval time.Duration) Option {
	return func(c *client) {
		c.flushInterval = interval
	}
}

// Close closes connections to hbase master and regionservers
func (c *client) Close() {
	// TODO: do we need a lock for metaRegionInfo and adminRegionInfo
	if mc := c.metaRegionInfo.Client(); mc != nil {
		mc.Close()
	}
	if ac := c.adminRegionInfo.Client(); ac != nil {
		ac.Close()
	}
	c.clients.closeAll()
}

// Scan retrieves the values specified in families from the given range.
func (c *client) Scan(s *hrpc.Scan) ([]*hrpc.Result, error) {
	var results []*pb.Result
	var scanres *pb.ScanResponse
	var rpc *hrpc.Scan
	ctx := s.Context()
	table := s.Table()
	families := s.Families()
	filters := s.Filter()
	startRow := s.StartRow()
	stopRow := s.StopRow()
	fromTs, toTs := s.TimeRange()
	maxVerions := s.MaxVersions()
	numberOfRows := s.NumberOfRows()
	for {
		// Make a new Scan RPC for this region
		if rpc != nil {
			// If it's not the first region, we want to start at whatever the
			// last region's StopKey was
			startRow = rpc.RegionStop()
		}

		// TODO: would be nicer to clone it in some way
		rpc, err := hrpc.NewScanRange(ctx, table, startRow, stopRow,
			hrpc.Families(families), hrpc.Filters(filters),
			hrpc.TimeRangeUint64(fromTs, toTs),
			hrpc.MaxVersions(maxVerions),
			hrpc.NumberOfRows(numberOfRows))
		if err != nil {
			return nil, err
		}

		res, err := c.sendRPC(rpc)
		if err != nil {
			return nil, err
		}
		scanres = res.(*pb.ScanResponse)
		results = append(results, scanres.Results...)

		// TODO: The more_results field of the ScanResponse object was always
		// true, so we should figure out if there's a better way to know when
		// to move on to the next region than making an extra request and
		// seeing if there were no results
		for len(scanres.Results) != 0 {
			rpc = hrpc.NewScanFromID(ctx, table, *scanres.ScannerId, rpc.Key())

			res, err = c.sendRPC(rpc)
			if err != nil {
				return nil, err
			}
			scanres = res.(*pb.ScanResponse)
			results = append(results, scanres.Results...)
		}

		rpc = hrpc.NewCloseFromID(ctx, table, *scanres.ScannerId, rpc.Key())
		if err != nil {
			return nil, err
		}
		res, err = c.sendRPC(rpc)

		// Check to see if this region is the last we should scan (either
		// because (1) it's the last region or (3) because its stop_key is
		// greater than or equal to the stop_key of this scanner provided
		// that (2) we're not trying to scan until the end of the table).
		// (1)
		if len(rpc.RegionStop()) == 0 ||
			// (2)                (3)
			len(stopRow) != 0 && bytes.Compare(stopRow, rpc.RegionStop()) <= 0 {
			// Do we want to be returning a slice of Result objects or should we just
			// put all the Cells into the same Result object?
			localResults := make([]*hrpc.Result, len(results))
			for idx, result := range results {
				localResults[idx] = hrpc.ToLocalResult(result)
			}
			return localResults, nil
		}
	}
}

func (c *client) Get(g *hrpc.Get) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(g)
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.GetResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a GetResponse")
	}

	return hrpc.ToLocalResult(r.Result), nil
}

func (c *client) Put(p *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(p)
}

func (c *client) Delete(d *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(d)
}

func (c *client) Append(a *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(a)
}

func (c *client) Increment(i *hrpc.Mutate) (int64, error) {
	r, err := c.mutate(i)
	if err != nil {
		return 0, err
	}

	if len(r.Cells) != 1 {
		return 0, fmt.Errorf("Increment returned %d cells, but we expected exactly one.",
			len(r.Cells))
	}

	val := binary.BigEndian.Uint64(r.Cells[0].Value)
	return int64(val), nil
}

func (c *client) mutate(m *hrpc.Mutate) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(m)
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.MutateResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a MutateResponse")
	}

	return hrpc.ToLocalResult(r.Result), nil
}

func (c *client) CheckAndPut(p *hrpc.Mutate, family string,
	qualifier string, expectedValue []byte) (bool, error) {
	cas, err := hrpc.NewCheckAndPut(p, family, qualifier, expectedValue)
	if err != nil {
		return false, err
	}

	pbmsg, err := c.sendRPC(cas)
	if err != nil {
		return false, err
	}

	r, ok := pbmsg.(*pb.MutateResponse)
	if !ok {
		return false, fmt.Errorf("sendRPC returned a %T instead of MutateResponse", pbmsg)
	}

	if r.Processed == nil {
		return false, fmt.Errorf("Protobuf in the response didn't contain the field "+
			"indicating whether the CheckAndPut was successful or not: %s", r)
	}

	return r.GetProcessed(), nil
}

func (c *client) checkProcedureWithBackoff(pContext context.Context, procID uint64) error {
	backoff := backoffStart
	ctx, cancel := context.WithTimeout(pContext, 30*time.Second)
	defer cancel()

	for {
		req := hrpc.NewGetProcedureState(ctx, procID)
		pbmsg, err := c.sendRPC(req)
		if err != nil {
			return err
		}

		statusRes, ok := pbmsg.(*pb.GetProcedureResultResponse)
		if !ok {
			return fmt.Errorf("sendRPC returned not a GetProcedureResultResponse")
		}

		switch statusRes.GetState() {
		case pb.GetProcedureResultResponse_NOT_FOUND:
			return fmt.Errorf("Procedure not found")
		case pb.GetProcedureResultResponse_FINISHED:
			return nil
		default:
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				return err
			}
		}
	}
}

func (c *client) CreateTable(t *hrpc.CreateTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.CreateTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a CreateTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) DeleteTable(t *hrpc.DeleteTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.DeleteTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a DeleteTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) EnableTable(t *hrpc.EnableTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.EnableTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a EnableTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) DisableTable(t *hrpc.DisableTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.DisableTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a DisableTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) sendRPC(rpc hrpc.Call) (proto.Message, error) {
	// Check the cache for a region that can handle this request
	var err error

	// block in case someone is updating regions.
	// for example someone is replacing a region with a new one,
	// we want to wait for that to finish so that we don't do
	// unnecessary region lookups in case that's our region.
	// TODO: is this bad?? We will unnecessarily slow down rpcs that have
	// regions in cache every time we add a new region,
	// so maybe it's fine to fail and take longer here
	c.regionsLock.Lock()
	reg := c.getRegionFromCache(rpc.Table(), rpc.Key())
	c.regionsLock.Unlock()
	if reg == nil {
		reg, err = c.findRegion(rpc.Context(), rpc.Table(), rpc.Key())
		if err != nil {
			return nil, err
		}
	}
	return c.sendRPCToRegion(rpc, reg)
}

func (c *client) sendRPCToRegion(rpc hrpc.Call, reg hrpc.RegionInfo) (proto.Message, error) {
	// check if the region is marked as available
	if reg.IsUnavailable() {
		return c.waitOnRegion(rpc, reg)
	}

	rpc.SetRegion(reg)

	// Queue the RPC to be sent to the region
	var err error
	if client := reg.Client(); client != nil {
		err = client.QueueRPC(rpc)
	} else {
		err = errors.New("no client for this region")
	}
	if err != nil {
		// There was an error queueing the RPC.
		// Mark the region as unavailable.
		if reg.MarkUnavailable() {
			// If this was the first goroutine to mark the region as
			// unavailable, start a goroutine to reestablish a connection
			go c.reestablishRegion(reg)
		}
		// Block until the region becomes available.
		return c.waitOnRegion(rpc, reg)
	}

	// Wait for the response
	var res hrpc.RPCResult
	select {
	case res = <-rpc.ResultChan():
	case <-rpc.Context().Done():
		return nil, ErrDeadline
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
		if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
			// The client won't be in the cache if this is the
			// meta or admin region
			c.clients.del(reg)
		}
		return c.waitOnRegion(rpc, reg)
	case region.UnrecoverableError:
		// If it was an unrecoverable error, the region client is
		// considered dead.
		if reg == c.metaRegionInfo || reg == c.adminRegionInfo {
			// If this is the admin client or the meta table, mark the
			// region as unavailable and start up a goroutine to
			// reconnect if it wasn't already marked as such.
			if reg.MarkUnavailable() {
				go c.reestablishRegion(reg)
			}
		} else {
			// Else this is a normal region. Mark all the regions
			// sharing this region's client as unavailable, and start
			// a goroutine to reconnect for each of them.
			downregions := c.clients.clientDown(reg)
			for _, downreg := range downregions {
				go c.reestablishRegion(downreg)
			}
		}

		// Fall through to the case of the region being unavailable,
		// which will result in blocking until it's available again.
		return c.waitOnRegion(rpc, reg)
	default:
		// RPC was successfully sent, or an unknown type of error
		// occurred. In either case, return the results.
		return res.Msg, res.Error
	}
}

func (c *client) waitOnRegion(rpc hrpc.Call, reg hrpc.RegionInfo) (proto.Message, error) {
	ch := reg.AvailabilityChan()
	if ch == nil {
		// WTF, this region is available? Maybe it was marked as such
		// since waitOnRegion was called.
		return c.sendRPC(rpc)
	}
	// The region is unavailable. Wait for it to become available,
	// or for the deadline to be exceeded.
	select {
	case <-ch:
		return c.sendRPC(rpc)
	case <-rpc.Context().Done():
		return nil, ErrDeadline
	}
}

// checkAndPutRegion checks cache for table and key and if no region exists,
// marks passed region unavailable and adds it to cache.
// This method is not concurrency safe, requires regionsLock.
func (c *client) checkAndPutRegion(table, key []byte, region hrpc.RegionInfo) hrpc.RegionInfo {
	if existing := c.getRegionFromCache(table, key); existing != nil {
		return existing
	}

	region.MarkUnavailable()
	removed := c.regions.put(region)
	for _, r := range removed {
		c.clients.del(r)
	}
	return region
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
			host, port, err = c.zkLookup(lookupCtx, zk.Master)
			cancel()
			reg = c.adminRegionInfo
		} else if bytes.Compare(table, c.metaRegionInfo.Table()) == 0 {
			host, port, err = c.zkLookup(lookupCtx, zk.Meta)
			cancel()
			reg = c.metaRegionInfo
		} else {
			reg, host, port, err = c.metaLookup(lookupCtx, table, key)
			cancel()
			if err == TableNotFound {
				return nil, "", 0, err
			}
		}
		if err == nil {
			return reg, host, port, nil
		}
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

	// Check that the region wasn't added to
	// the cache while we were looking it up.
	c.regionsLock.Lock()
	if r := c.checkAndPutRegion(table, key, reg); reg != r {
		c.regionsLock.Unlock()
		return r, nil
	}
	c.regionsLock.Unlock()

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
	if region == nil || !bytes.Equal(table, region.Table()) {
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
	if !bytes.Equal(table, reg.Table()) {
		// This would indicate a bug in HBase.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong table!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	} else if len(reg.StopKey()) != 0 &&
		bytes.Compare(key, reg.StopKey()) >= 0 {
		// This would indicate a hole in the meta table.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong region!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	}
	return reg, host, port, nil
}

func (c *client) reestablishRegion(reg hrpc.RegionInfo) {
	c.establishRegion(reg, "", 0)
}

func (c *client) establishRegion(reg hrpc.RegionInfo, host string, port uint16) {
	backoff := backoffStart
	var err error
	for {
		if host == "" && port == 0 {
			// need to look up region and address of the regionserver
			originalReg := reg
			// lookup region forever until we get it or we learn that it doesn't exist
			reg, host, port, err = c.lookupRegion(context.Background(),
				originalReg.Table(), originalReg.StartKey())
			if err == TableNotFound {
				// region doesn't exist, delete it from caches
				c.regions.del(originalReg.StartKey())
				c.clients.del(originalReg)
				originalReg.MarkAvailable()
				return
			} else if err != nil {
				log.Fatalf("Unknow error occured when looking up region: %v", err)
			}

			if bytes.Compare(reg.Name(), originalReg.Name()) != 0 {
				// there's a new region, we should remove the old one
				// and add this one unless someone else has already done so
				c.regionsLock.Lock()
				// delete original since we have a new one
				c.regions.del(originalReg.StartKey())

				// Check that the region wasn't added to
				// the cache while we were looking it up.
				// For example if region merge happened and some dude
				// was looking up the region before the original and found the
				// same one as we are and could have added it to the cache
				if r := c.checkAndPutRegion(reg.Table(), reg.StartKey(), reg); reg != r {
					// looks like someone already found this region already,
					// it's their responsibility to reestablish it
					c.regionsLock.Unlock()
					// let rpcs know that they can retry
					originalReg.MarkAvailable()
					return
				}
				c.regionsLock.Unlock()

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
			// set region client so that as soon as we mark it available,
			// concurrent readers are able to find the client
			reg.SetClient(client)
			if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
				c.clients.put(client, reg)
			}
			reg.MarkAvailable()
			return
		}

		// reset address because we weren't able to connect to it,
		// should look up again
		host, port = "", 0

		// This will be hit if there was an error connecting to the region
		backoff, err = sleepAndIncreaseBackoff(context.Background(), backoff)
		if err != nil {
			log.Fatalf("This error should never happen: %v", err)
		}
	}
}

func (c *client) establishRegionClient(reg hrpc.RegionInfo,
	host string, port uint16) (hrpc.RegionClient, error) {
	if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
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
	clientCtx, cancel := context.WithTimeout(context.Background(), regionLookupTimeout)
	defer cancel()
	return region.NewClient(clientCtx, host, port, clientType,
		c.rpcQueueSize, c.flushInterval)
}

func sleepAndIncreaseBackoff(ctx context.Context, backoff time.Duration) (time.Duration, error) {
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
		host, port, err := c.zkClient.LocateResource(resource)
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
