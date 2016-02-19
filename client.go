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

type newRegResult struct {
	Client *region.Client
	Err    error
}

// client -> region cache. Used to quickly look up all the
// regioninfos that map to a specific client
type clientRegionCache struct {
	m sync.Mutex

	regions map[hrpc.RegionClient][]hrpc.RegionInfo
}

func (rcc *clientRegionCache) put(r hrpc.RegionInfo, c hrpc.RegionClient) {
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
	defer rcc.m.Unlock()

	c := r.GetClient()
	if c != nil {
		r.SetClient(nil)

		var index int
		for i, reg := range rcc.regions[c] {
			if reg == r {
				index = i
			}
		}
		rcc.regions[c] = append(
			rcc.regions[c][:index],
			rcc.regions[c][index+1:]...)
	}
}

func (rcc *clientRegionCache) clientDown(reg hrpc.RegionInfo) []hrpc.RegionInfo {
	rcc.m.Lock()
	defer rcc.m.Unlock()

	var downregions []hrpc.RegionInfo
	c := reg.GetClient()
	for _, sharedReg := range rcc.regions[c] {
		succ := sharedReg.MarkUnavailable()
		sharedReg.SetClient(nil)
		if succ {
			downregions = append(downregions, sharedReg)
		}
	}
	delete(rcc.regions, c)
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
	// TODO: It would be nice if we could do just enum.Get() to avoid the
	// unnecessary cost of seeking to the next entry.
	krc.m.Unlock()
	if err != nil {
		return nil, nil
	}
	return k.([]byte), v.(hrpc.RegionInfo)
}

func (krc *keyRegionCache) put(key []byte, reg hrpc.RegionInfo) hrpc.RegionInfo {
	krc.m.Lock()
	// TODO: We need to remove all the entries that are overlap with the range
	// of the new region being added here, if any.
	oldV, _ := krc.regions.Put(key, func(interface{}, bool) (interface{}, bool) {
		return reg, true
	})
	krc.m.Unlock()
	if oldV == nil {
		return nil
	}
	return oldV.(hrpc.RegionInfo)
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

	// The timeout before flushing the RPC queue in the region client
	flushInterval time.Duration
}

// Client a regular HBase client
type Client interface {
	CheckTable(ctx context.Context, table string) error
	Scan(s *hrpc.Scan) ([]*hrpc.Result, error)
	Get(g *hrpc.Get) (*hrpc.Result, error)
	Put(p *hrpc.Mutate) (*hrpc.Result, error)
	Delete(d *hrpc.Mutate) (*hrpc.Result, error)
	Append(a *hrpc.Mutate) (*hrpc.Result, error)
	Increment(i *hrpc.Mutate) (int64, error)
	CheckAndPut(p *hrpc.Mutate, family string, qualifier string,
		expectedValue []byte) (bool, error)
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
		metaRegionInfo: &region.Info{
			Table:   []byte("hbase:meta"),
			Name:    []byte("hbase:meta,,1"),
			StopKey: []byte{},
		},
		adminRegionInfo: &region.Info{},
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

// CheckTable returns an error if the given table name doesn't exist.
func (c *client) CheckTable(ctx context.Context, table string) error {
	getStr, err := hrpc.NewGetStr(ctx, table, "theKey")
	if err == nil {
		_, err = c.SendRPC(getStr)
	}
	return err
}

// Scan retrieves the values specified in families from the given range.
func (c *client) Scan(s *hrpc.Scan) ([]*hrpc.Result, error) {
	var results []*pb.Result
	var scanres *pb.ScanResponse
	var rpc *hrpc.Scan
	ctx := s.GetContext()
	table := s.Table()
	families := s.GetFamilies()
	filters := s.GetFilter()
	startRow := s.GetStartRow()
	stopRow := s.GetStopRow()
	for {
		// Make a new Scan RPC for this region
		if rpc != nil {
			// If it's not the first region, we want to start at whatever the
			// last region's StopKey was
			startRow = rpc.GetRegionStop()
		}

		rpc, err := hrpc.NewScanRange(ctx, table, startRow, stopRow,
			hrpc.Families(families), hrpc.Filters(filters))
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
		if len(rpc.GetRegionStop()) == 0 ||
			// (2)                (3)
			len(stopRow) != 0 && bytes.Compare(stopRow, rpc.GetRegionStop()) <= 0 {
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

	return c.checkProcedureWithBackoff(t.GetContext(), r.GetProcId())
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

	return c.checkProcedureWithBackoff(t.GetContext(), r.GetProcId())
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

	return c.checkProcedureWithBackoff(t.GetContext(), r.GetProcId())
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

	return c.checkProcedureWithBackoff(t.GetContext(), r.GetProcId())
}

// Could be removed in favour of above
func (c *client) SendRPC(rpc hrpc.Call) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(rpc)

	var rsp *hrpc.Result
	switch r := pbmsg.(type) {
	case *pb.GetResponse:
		rsp = hrpc.ToLocalResult(r.Result)
	case *pb.MutateResponse:
		rsp = hrpc.ToLocalResult(r.Result)
	}

	return rsp, err
}

func (c *client) sendRPC(rpc hrpc.Call) (proto.Message, error) {
	// Check the cache for a region that can handle this request
	reg := c.getRegionFromCache(rpc.Table(), rpc.Key())
	if reg != nil {
		return c.sendRPCToRegion(rpc, reg)
	} else {
		return c.findRegionForRPC(rpc)
	}
}

func (c *client) sendRPCToRegion(rpc hrpc.Call, reg hrpc.RegionInfo) (proto.Message, error) {
	client := reg.GetClient()
	// On the first sendRPC to the meta or admin regions, a goroutine must be
	// manually kicked off for the meta or admin region client
	if reg == c.adminRegionInfo && client == nil && !c.adminRegionInfo.IsUnavailable() ||
		reg == c.metaRegionInfo && client == nil && !c.metaRegionInfo.IsUnavailable() {
		c.regionsLock.Lock()
		if reg.MarkUnavailable() {
			go c.reestablishRegion(reg)
		}
		c.regionsLock.Unlock()
	}
	// The region was in the cache, check
	// if the region is marked as available
	if reg.IsUnavailable() {
		return c.waitOnRegion(rpc, reg)
	}

	rpc.SetRegion(reg)

	// Queue the RPC to be sent to the region
	var err error
	if client == nil {
		err = errors.New("no client for this region")
	} else {
		err = client.QueueRPC(rpc)
	}

	if err != nil {
		// There was an error queueing the RPC.
		// Mark the region as unavailable.
		first := reg.MarkUnavailable()
		// If this was the first goroutine to mark the region as
		// unavailable, start a goroutine to reestablish a connection
		if first {
			go c.reestablishRegion(reg)
		}
		// Block until the region becomes available.
		return c.waitOnRegion(rpc, reg)
	}

	// Wait for the response
	var res hrpc.RPCResult
	select {
	case res = <-rpc.GetResultChan():
	case <-rpc.GetContext().Done():
		return nil, ErrDeadline
	}

	// Check for errors
	if _, ok := res.Error.(region.RetryableError); ok {
		// There's an error specific to this region, but
		// our region client is fine. Mark this region as
		// unavailable (as opposed to all regions sharing
		// the client), and start a goroutine to reestablish
		// it.
		first := reg.MarkUnavailable()
		if first {
			go c.reestablishRegion(reg)
		}
		if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
			// The client won't be in the cache if this is the
			// meta or admin region
			c.clients.del(reg)
		}
		return c.waitOnRegion(rpc, reg)
	} else if _, ok := res.Error.(region.UnrecoverableError); ok {
		// If it was an unrecoverable error, the region client is
		// considered dead.
		if reg == c.metaRegionInfo || reg == c.adminRegionInfo {
			// If this is the admin client or the meta table, mark the
			// region as unavailable and start up a goroutine to
			// reconnect if it wasn't already marked as such.
			first := reg.MarkUnavailable()
			if first {
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
	} else {
		// RPC was successfully sent, or an unknown type of error
		// occurred. In either case, return the results.
		return res.Msg, res.Error
	}
}

func (c *client) waitOnRegion(rpc hrpc.Call, reg hrpc.RegionInfo) (proto.Message, error) {
	ch := reg.GetAvailabilityChan()
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
	case <-rpc.GetContext().Done():
		return nil, ErrDeadline
	}
}

func (c *client) findRegionForRPC(rpc hrpc.Call) (proto.Message, error) {
	// The region was not in the cache, it
	// must be looked up in the meta table

	backoff := backoffStart
	ctx := rpc.GetContext()
	for {
		// Look up the region in the meta table
		reg, host, port, err := c.locateRegion(ctx, rpc.Table(), rpc.Key())

		if err != nil {
			if err == TableNotFound {
				return nil, err
			}
			// There was an error with the meta table. Let's sleep for some
			// backoff amount and retry.
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				return nil, err
			}
			continue
		}

		// Check that the region wasn't added to
		// the cache while we were looking it up.
		c.regionsLock.Lock()

		if existing := c.getRegionFromCache(rpc.Table(), rpc.Key()); existing != nil {
			// The region was added to the cache while we were looking it
			// up. Send the RPC to the region that was in the cache.
			c.regionsLock.Unlock()
			return c.sendRPCToRegion(rpc, existing)
		}

		// The region wasn't added to the cache while we were looking it
		// up. Mark this one as unavailable and add it to the cache.
		reg.MarkUnavailable()
		c.regions.put(reg.GetName(), reg)

		c.regionsLock.Unlock()

		// Start a goroutine to connect to the region
		go c.establishRegion(reg, host, port)

		// Wait for the new region to become
		// available, and then send the RPC
		return c.waitOnRegion(rpc, reg)
	}
}

// Searches in the regions cache for the region hosting the given row.
func (c *client) getRegionFromCache(table, key []byte) hrpc.RegionInfo {
	if c.clientType == adminClient {
		return c.adminRegionInfo
	} else if bytes.Equal(table, metaTableName) {
		return c.metaRegionInfo
	}
	regionName := createRegionSearchKey(table, key)
	regionKey, region := c.regions.get(regionName)
	if region == nil || !isCacheKeyForTable(table, regionKey) {
		return nil
	}

	if len(region.GetStopKey()) != 0 &&
		// If the stop key is an empty byte array, it means this region is the
		// last region for this table and this key ought to be in that region.
		bytes.Compare(key, region.GetStopKey()) >= 0 {
		return nil
	}

	return region
}

// Checks whether or not the given cache key is for the given table.
func isCacheKeyForTable(table, cacheKey []byte) bool {
	// Check we found an entry that's really for the requested table.
	for i := 0; i < len(table); i++ {
		if table[i] != cacheKey[i] { // This table isn't in the map, we found
			return false // a key which is for another table.
		}
	}

	// Make sure we didn't find another key that's for another table
	// whose name is a prefix of the table name we were given.
	return cacheKey[len(table)] == ','
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

// Locates the region in which the given row key for the given table is.
func (c *client) locateRegion(ctx context.Context,
	table, key []byte) (hrpc.RegionInfo, string, uint16, error) {

	metaKey := createRegionSearchKey(table, key)
	rpc, err := hrpc.NewGetBefore(ctx, metaTableName, metaKey, hrpc.Families(infoFamily))
	if err != nil {
		return nil, "", 0, err
	}
	rpc.SetRegion(c.metaRegionInfo)
	resp, err := c.sendRPC(rpc)

	if err != nil {
		ch := c.metaRegionInfo.GetAvailabilityChan()
		if ch != nil {
			select {
			case <-ch:
				return c.locateRegion(ctx, table, key)
			case <-rpc.GetContext().Done():
				return nil, "", 0, ErrDeadline
			}
		} else {
			return nil, "", 0, err
		}
	}

	metaRow := resp.(*pb.GetResponse)
	if metaRow.Result == nil {
		return nil, "", 0, TableNotFound
	}

	reg, host, port, err := region.ParseRegionInfo(metaRow)
	if err != nil {
		return nil, "", 0, err
	}
	if !bytes.Equal(table, reg.GetTable()) {
		// This would indicate a bug in HBase.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong table!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	} else if len(reg.GetStopKey()) != 0 &&
		bytes.Compare(key, reg.GetStopKey()) >= 0 {
		// This would indicate a hole in the meta table.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong region!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	}
	return reg, host, port, nil
}

func (c *client) reestablishRegion(reg hrpc.RegionInfo) {
	c.establishRegion(reg, "", 0)
}

func (c *client) establishRegion(originalReg hrpc.RegionInfo, host string, port uint16) {
	var err error
	reg := originalReg
	backoff := backoffStart

	for {
		ctx, _ := context.WithTimeout(context.Background(), regionLookupTimeout)
		if port != 0 && err == nil {
			// If this isn't the admin or meta region, check if a client
			// for this host/port already exists
			if c.clientType != adminClient && reg != c.metaRegionInfo {
				client := c.clients.checkForClient(host, port)
				if client != nil {
					// There's already a client, add it to the
					// region and mark it as available.
					reg.SetClient(client)
					c.clients.put(reg, client)
					originalReg.MarkAvailable()
					return
				}
			}
			// Make this channel buffered so that if we time out we don't
			// block the newRegion goroutine forever.
			ch := make(chan newRegResult, 1)
			var clientType region.ClientType
			if c.clientType == standardClient {
				clientType = region.RegionClient
			} else {
				clientType = region.MasterClient
			}
			go newRegionClient(ctx, ch, clientType, host, port, c.rpcQueueSize, c.flushInterval)

			select {
			case res := <-ch:
				if res.Err == nil {
					reg.SetClient(res.Client)
					if c.clientType != adminClient && reg != c.metaRegionInfo {
						c.clients.put(reg, res.Client)
						if reg != originalReg {
							// Here `reg' is guaranteed to be available, so we
							// must publish the region->client mapping first,
							// because as soon as we add it to the key->region
							// mapping here, concurrent readers are gonna want
							// to find the client.
							c.regions.put(reg.GetName(), reg)
						}
					}
					originalReg.MarkAvailable()
					return
				} else {
					err = res.Err
				}
			case <-ctx.Done():
				err = ErrDeadline
			}
		}
		if err != nil {
			if err == TableNotFound {
				c.regions.del(originalReg.GetName())
				originalReg.MarkAvailable()
				return
			}
			// This will be hit if either there was an error locating the
			// region, or the region was located but there was an error
			// connecting to it.
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				continue
			}
		}
		if c.clientType == adminClient {
			host, port, err = c.zkLookup(ctx, zk.Master)
		} else if reg == c.metaRegionInfo {
			host, port, err = c.zkLookup(ctx, zk.Meta)
		} else {
			reg, host, port, err = c.locateRegion(ctx, originalReg.GetTable(),
				originalReg.GetStartKey())
		}
	}
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

func newRegionClient(ctx context.Context, ret chan newRegResult, clientType region.ClientType,
	host string, port uint16, queueSize int, queueTimeout time.Duration) {
	c, e := region.NewClient(host, port, clientType, queueSize, queueTimeout)
	select {
	case ret <- newRegResult{c, e}:
		// Hooray!
	case <-ctx.Done():
		// We timed out, too bad, nobody expects this client anymore, ditch it.
		c.Close()
	}
}

// zkResult contains the result of a ZooKeeper lookup (when we're looking for
// the meta region or the HMaster).
type zkResult struct {
	host string
	port uint16
	err  error
}

// Asynchronously looks up the meta region or HMaster in ZooKeeper.
func (c *client) zkLookup(ctx context.Context, res zk.ResourceName) (string, uint16, error) {
	// We make this a buffered channel so that if we stop waiting due to a
	// timeout, we won't block the zkLookupSync() that we start in a
	// separate goroutine.
	reschan := make(chan zkResult, 1)
	go c.zkLookupSync(res, reschan)
	select {
	case res := <-reschan:
		return res.host, res.port, res.err
	case <-ctx.Done():
		return "", 0, ErrDeadline
	}
}

// Synchronously looks up the meta region or HMaster in ZooKeeper.
func (c *client) zkLookupSync(res zk.ResourceName, reschan chan<- zkResult) {
	host, port, err := zk.LocateResource(c.zkquorum, res)
	// This is guaranteed to never block as the channel is always buffered.
	reschan <- zkResult{host, port, err}
}
