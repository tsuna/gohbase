// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build testing,!integration

package gohbase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	atest "github.com/aristanetworks/goarista/test"
	"github.com/cznic/b"
	"github.com/golang/mock/gomock"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/test"
	"github.com/tsuna/gohbase/test/mock"
	mockRegion "github.com/tsuna/gohbase/test/mock/region"
	mockZk "github.com/tsuna/gohbase/test/mock/zk"
	"github.com/tsuna/gohbase/zk"
)

func newMockClient(zkClient zk.Client) *client {
	return &client{
		clientType: region.RegionClient,
		regions:    keyRegionCache{regions: b.TreeNew(region.CompareGeneric)},
		clients: clientRegionCache{
			regions: make(map[hrpc.RegionClient]map[hrpc.RegionInfo]struct{}),
		},
		rpcQueueSize:  defaultRPCQueueSize,
		flushInterval: defaultFlushInterval,
		metaRegionInfo: region.NewInfo(0, []byte("hbase"), []byte("meta"),
			[]byte("hbase:meta,,1"), nil, nil),
		zkTimeout:           defaultZkTimeout,
		zkClient:            zkClient,
		metaLookupLimiter:   rate.NewLimiter(metaLimit, metaBurst),
		regionLookupTimeout: region.DefaultLookupTimeout,
		regionReadTimeout:   region.DefaultReadTimeout,
	}
}

func TestSendRPCSanity(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	// we expect to ask zookeeper for where metaregion is
	zkClient := mockZk.NewMockClient(ctrl)
	zkClient.EXPECT().LocateResource(zk.Meta).Return("regionserver:1", nil).MinTimes(1)
	c := newMockClient(zkClient)

	// ask for "theKey" in table "test"
	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().Context().Return(context.Background()).AnyTimes()
	mockCall.EXPECT().Table().Return([]byte("test")).AnyTimes()
	mockCall.EXPECT().Key().Return([]byte("theKey")).AnyTimes()
	mockCall.EXPECT().SetRegion(gomock.Any()).AnyTimes()
	result := make(chan hrpc.RPCResult, 1)

	// pretend that response is successful
	expMsg := &pb.ScanResponse{}
	result <- hrpc.RPCResult{Msg: expMsg}
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	msg, err := c.SendRPC(mockCall)
	if err != nil {
		t.Fatal(err)
	}
	if diff := atest.Diff(expMsg, msg); diff != "" {
		t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
			expMsg, msg, diff)
	}

	if len(c.clients.regions) != 2 {
		t.Errorf("Expected 2 clients in cache, got %d", len(c.clients.regions))
	}

	// addr -> region name
	expClients := map[string]string{
		"regionserver:1": "hbase:meta,,1",
		"regionserver:2": "test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.",
	}

	// make sure those are the right clients
	for c, rs := range c.clients.regions {
		name, ok := expClients[c.Addr()]
		if !ok {
			t.Errorf("Got unexpected client %s in cache", c.Addr())
			continue
		}
		if len(rs) != 1 {
			t.Errorf("Expected to have only 1 region in cache for client %s", c.Addr())
			continue
		}
		for r := range rs {
			if string(r.Name()) != name {
				t.Errorf("Unexpected name of region %q for client %s, expected %q",
					r.Name(), c.Addr(), name)
			}
			// check bidirectional mapping, they have to be the same objects
			rc := r.Client()
			if c != rc {
				t.Errorf("Invalid bidirectional mapping: forward=%s, backward=%s",
					c.Addr(), rc.Addr())
			}
		}
	}

	if c.regions.regions.Len() != 1 {
		// expecting only one region because meta isn't in cache, it's hard-coded
		t.Errorf("Expected 1 regions in cache, got %d", c.regions.regions.Len())
	}
}

func TestReestablishRegionSplit(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	// we expect to ask zookeeper for where metaregion is
	c := newMockClient(mockZk.NewMockClient(ctrl))

	// inject a fake regionserver client and fake region into cache
	origlReg := region.NewInfo(
		0,
		nil,
		[]byte("test1"),
		[]byte("test1,,1434573235908.56f833d5569a27c7a43fbf547b4924a4."),
		nil,
		nil,
	)
	rc1, err := region.NewClient(
		context.Background(), "regionserver:1", region.RegionClient,
		0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}
	// pretend regionserver:1 has meta table
	c.metaRegionInfo.SetClient(rc1)
	// "test1" is at the moment at regionserver:1
	origlReg.SetClient(rc1)
	// marking unavailable to simulate error
	origlReg.MarkUnavailable()
	c.regions.put(origlReg)
	c.clients.put(rc1, origlReg)
	c.clients.put(rc1, c.metaRegionInfo)

	c.reestablishRegion(origlReg)

	if len(c.clients.regions) != 1 {
		t.Errorf("Expected 1 client in cache, got %d", len(c.clients.regions))
	}

	expRegs := map[string]struct{}{
		"hbase:meta,,1": struct{}{},
		"test1,,1480547738107.825c5c7e480c76b73d6d2bad5d3f7bb8.": struct{}{},
	}

	// make sure those are the right clients
	for rc, rs := range c.clients.regions {
		if rc.Addr() != "regionserver:1" {
			t.Errorf("Got unexpected client %s in cache", rc.Addr())
			break
		}

		// check that we have correct regions in the client
		gotRegs := map[string]struct{}{}
		for r := range rs {
			gotRegs[string(r.Name())] = struct{}{}
			// check that regions have correct client
			if r.Client() != rc1 {
				t.Errorf("Invalid bidirectional mapping: forward=%s, backward=%s",
					r.Client().Addr(), rc1.Addr())
			}
		}
		if diff := atest.Diff(expRegs, gotRegs); diff != "" {
			t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
				expRegs, gotRegs, diff)
		}

		// check that we still have the same client that we injected
		if rc != rc1 {
			t.Errorf("Invalid bidirectional mapping: forward=%s, backward=%s",
				rc.Addr(), rc1.Addr())
		}
	}

	if c.regions.regions.Len() != 1 {
		// expecting only one region because meta isn't in cache, it's hard-coded
		t.Errorf("Expected 1 regions in cache, got %d", c.regions.regions.Len())
	}

	// check the we have correct region in regions cache
	newRegIntf, ok := c.regions.regions.Get(
		[]byte("test1,,1480547738107.825c5c7e480c76b73d6d2bad5d3f7bb8."))
	if !ok {
		t.Error("Expected region is not in the cache")
	}

	// check new region is available
	newReg, ok := newRegIntf.(hrpc.RegionInfo)
	if !ok {
		t.Error("Expected hrpc.RegionInfo")
	}
	if newReg.IsUnavailable() {
		t.Error("Expected new region to be available")
	}

	// check old region is available and it's client is empty since we
	// need to release all the waiting callers
	if origlReg.IsUnavailable() {
		t.Error("Expected original region to be available")
	}

	if origlReg.Client() != nil {
		t.Error("Expected original region to have no client")
	}
}

func TestThrottleRegionLookups(t *testing.T) {
	t.Skip("throttling is broken")
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := newMockClient(nil)
	numOK := int32(0)

	rc := mockRegion.NewMockRegionClient(ctrl)
	rc.EXPECT().String().Return("mock region client").AnyTimes()
	rc.EXPECT().QueueRPC(gomock.Any()).AnyTimes().Do(func(rpc hrpc.Call) {
		atomic.AddInt32(&numOK, 1)
		rpc.ResultChan() <- hrpc.RPCResult{}
	})
	c.metaRegionInfo.SetClient(rc)
	c.clients.put(rc, c.metaRegionInfo)
	ctx := context.Background()
	table, key := []byte("yolo"), []byte("swag")

	start := time.Now()
	end := start.Add(time.Second)
	var wg sync.WaitGroup
	for time.Now().Before(end) {
		wg.Add(1)
		go func() {
			c.metaLookup(ctx, table, key)
			wg.Done()
		}()
		// this will offer ~2,000 requests per second
		// while we only allow 100 request per 100 milliseconds
		time.Sleep(500 * time.Microsecond)
	}
	wg.Wait()
	elapsed := time.Since(start)
	ideal := 1 + (100 * float64(elapsed) / float64(100*time.Millisecond))

	// We should never get more requests than allowed.
	if want := int32(ideal + 1); numOK > want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
}

func TestReestablishRegionNSRE(t *testing.T) {
	c := newMockClient(nil)
	origlReg := region.NewInfo(0, nil, []byte("nsre"),
		[]byte("nsre,,1434573235908.56f833d5569a27c7a43fbf547b4924a4."), nil, nil)
	// inject a fake regionserver client and fake region into cache
	rc1, err := region.NewClient(context.Background(), "regionserver:1", region.RegionClient,
		0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}
	// pretend regionserver:1 has meta table
	c.metaRegionInfo.SetClient(rc1)
	c.clients.put(rc1, c.metaRegionInfo)
	// "nsre" is at the moment at regionserver:1
	origlReg.SetClient(rc1)
	c.clients.put(rc1, origlReg)
	// marking unavailable to simulate error
	origlReg.MarkUnavailable()
	c.regions.put(origlReg)

	c.reestablishRegion(origlReg)

	if len(c.clients.regions) != 1 {
		t.Errorf("Expected 1 client in cache, got %d", len(c.clients.regions))
	}

	if c.regions.regions.Len() != 1 {
		// expecting only one region because meta isn't in cache, it's hard-coded
		t.Errorf("Expected 1 regions in cache, got %d", c.regions.regions.Len())
	}

	// check the we have the region in regions cache
	_, ok := c.regions.regions.Get(
		[]byte("nsre,,1434573235908.56f833d5569a27c7a43fbf547b4924a4."))
	if !ok {
		t.Error("Expected region is not in the cache")
	}

	// check region is available and it's client is empty since we
	// need to release all the waiting callers
	if origlReg.IsUnavailable() {
		t.Error("Expected original region to be available")
	}

	if origlReg.Client() != rc1 {
		t.Error("Expected original region the same client")
	}
}

func TestEstablishClientConcurrent(t *testing.T) {
	// test that the same client isn't added when establishing it concurrently
	// if there's a race, this test will only fail sometimes
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	// we expect to ask zookeeper for where metaregion is
	c := newMockClient(mockZk.NewMockClient(ctrl))
	// pre-create fake regions to establish
	numRegions := 1000
	regions := make([]hrpc.RegionInfo, numRegions)
	for i := range regions {
		r := region.NewInfo(
			0,
			nil,
			[]byte("test"),
			[]byte(fmt.Sprintf("test,%d,1434573235908.yoloyoloyoloyoloyoloyoloyoloyolo.", i)),
			nil, nil)
		r.MarkUnavailable()
		regions[i] = r
	}

	// all of the regions have the same region client
	var wg sync.WaitGroup
	for _, r := range regions {
		r := r
		wg.Add(1)
		go func() {
			c.establishRegion(r, "regionserver:1")
			wg.Done()
		}()
	}
	wg.Wait()

	if len(c.clients.regions) != 1 {
		t.Fatalf("Expected to have only 1 client in cache, got %d", len(c.clients.regions))
	}

	for rc, rs := range c.clients.regions {
		if len(rs) != numRegions {
			t.Errorf("Expected to have %d regions, got %d", numRegions, len(rs))
		}
		// check that all regions have the same client set and are available
		for _, r := range regions {
			if r.Client() != rc {
				t.Errorf("Region %q has unexpected client %s", r.Name(), rc.Addr())
			}
			if r.IsUnavailable() {
				t.Errorf("Expected region %s to be available", r.Name())
			}
		}
	}
}

func TestEstablishServerErrorDuringProbe(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := newMockClient(nil)

	rc, err := region.NewClient(
		context.Background(), "regionserver:0", region.RegionClient,
		0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}

	// pretend regionserver:0 has meta table
	c.metaRegionInfo.SetClient(rc)
	c.clients.put(rc, c.metaRegionInfo)

	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().Context().Return(context.Background()).AnyTimes()
	mockCall.EXPECT().Table().Return([]byte("down")).AnyTimes()
	mockCall.EXPECT().Key().Return([]byte("yolo")).AnyTimes()
	mockCall.EXPECT().SetRegion(gomock.Any()).AnyTimes()
	result := make(chan hrpc.RPCResult, 1)
	// pretend that response is successful
	expMsg := &pb.GetResponse{}
	result <- hrpc.RPCResult{Msg: expMsg}
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	msg, err := c.SendRPC(mockCall)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if diff := atest.Diff(expMsg, msg); diff != "" {
		t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
			expMsg, msg, diff)
	}

	if len(c.clients.regions) != 2 {
		t.Errorf("expected to have 2 clients in cache, got %d", len(c.clients.regions))
	}
}

// TestSendRPCToRegionClientDownDelayed check that the we don't replace a newly
// looked up client (by other goroutine) in clients cache if we are too slow
// at finding out that our old client died.
func TestSendRPCToRegionClientDownDelayed(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	// we don't expect any calls to zookeeper
	c := newMockClient(nil)

	// create region with mock client
	origlReg := region.NewInfo(
		0, nil, []byte("test1"),
		[]byte("test1,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		nil, nil)
	rc := mockRegion.NewMockRegionClient(ctrl)
	rc.EXPECT().String().Return("mock region client").AnyTimes()
	origlReg.SetClient(rc)
	c.regions.put(origlReg)
	c.clients.put(rc, origlReg)

	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().SetRegion(origlReg).Times(1)
	mockCall.EXPECT().Context().Return(context.Background())
	result := make(chan hrpc.RPCResult, 1)
	mockCall.EXPECT().ResultChan().Return(result).Times(1)

	rc2 := mockRegion.NewMockRegionClient(ctrl)
	rc2.EXPECT().String().Return("mock region client").AnyTimes()
	rc.EXPECT().QueueRPC(mockCall).Times(1).Do(func(rpc hrpc.Call) {
		// remove old client from clients cache
		c.clients.clientDown(rc)
		// replace client in region with new client
		// this simulate other rpc toggling client reestablishment
		c.regions.put(origlReg)
		c.clients.put(rc2, origlReg)
		origlReg.SetClient(rc2)

		// return ServerError from QueueRPC, to emulate dead client
		result <- hrpc.RPCResult{Error: region.ServerError{}}
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := c.sendRPCToRegion(mockCall, origlReg)
		switch err.(type) {
		case region.ServerError, region.NotServingRegionError:
		default:
			t.Errorf("Got unexpected error: %v", err)
		}
		wg.Done()
	}()

	wg.Wait()

	// check that we did not down new client
	if len(c.clients.regions) != 1 {
		t.Errorf("There are %d cached clients", len(c.clients.regions))
	}
	_, ok := c.clients.regions[rc2]
	if !ok {
		t.Error("rc2 is not in clients cache")
	}
}

func TestReestablishDeadRegion(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	// setting zookeeper client to nil because we don't
	// expect for it to be called
	c := newMockClient(nil)
	rc1, err := region.NewClient(
		context.Background(), "regionserver:0", region.RegionClient,
		0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}
	// here we assume that this region was removed from
	// regions cache and thereby is considered dead
	reg := region.NewInfo(
		0,
		nil,
		[]byte("test"),
		[]byte("test,,1434573235908.yoloyoloyoloyoloyoloyoloyoloyolo."),
		nil, nil)
	reg.MarkDead()

	// pretend regionserver:0 has meta table
	c.metaRegionInfo.SetClient(rc1)
	c.clients.put(rc1, c.metaRegionInfo)
	c.clients.put(rc1, reg)

	reg.MarkUnavailable()

	// pretend that there's a race condition right after we removed region
	// from regions cache a client dies that we haven't removed from
	// clients cache yet
	c.reestablishRegion(reg) // should exit TODO: check fast

	// should not put anything in cache, we specifically get a region with new name
	if c.regions.regions.Len() != 0 {
		t.Errorf("Expected to have no region in cache, got %d", c.regions.regions.Len())
	}

	// should not establish any clients
	if len(c.clients.regions) != 1 {
		t.Errorf("Expected to have 1 client in cache, got %d", len(c.clients.regions))
	}

	if reg.Client() != nil {
		t.Error("Expected to have no client established")
	}

	// should have reg as available
	if reg.IsUnavailable() {
		t.Error("Expected region to be available")
	}
}

func TestFindRegion(t *testing.T) {
	// TODO: check regions are deleted from client's cache
	// when regions are replaced
	tcases := []struct {
		before    []hrpc.RegionInfo
		after     []string
		establish bool
		regName   string
		err       error
	}{
		{ // nothing in cache
			before:    nil,
			after:     []string{"test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4."},
			establish: true,
			regName:   "test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.",
		},
		{ // older region in cache
			before: []hrpc.RegionInfo{
				region.NewInfo(1, nil, []byte("test"),
					[]byte("test,,1.yoloyoloyoloyoloyoloyoloyoloyolo."), nil, nil),
			},
			after:     []string{"test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4."},
			establish: true,
			regName:   "test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.",
		},
		{ // younger region in cache
			before: []hrpc.RegionInfo{
				region.NewInfo(9999999999999, nil, []byte("test"),
					[]byte("test,,9999999999999.yoloyoloyoloyoloyoloyoloyoloyolo."), nil, nil),
			},
			after:     []string{"test,,9999999999999.yoloyoloyoloyoloyoloyoloyoloyolo."},
			establish: false,
		},
		{ // overlapping younger region in cache, passed key is not in that region
			before: []hrpc.RegionInfo{
				region.NewInfo(9999999999999, nil, []byte("test"),
					[]byte("test,,9999999999999.yoloyoloyoloyoloyoloyoloyoloyolo."),
					nil, []byte("foo")),
			},
			after:     []string{"test,,9999999999999.yoloyoloyoloyoloyoloyoloyoloyolo."},
			establish: false,
		},
	}

	ctrl := test.NewController(t)
	defer ctrl.Finish()
	// setting zookeeper client to nil because we don't
	// expect for it to be called
	c := newMockClient(nil)
	rc, err := region.NewClient(
		context.Background(), "regionserver:0", region.RegionClient,
		0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}
	// pretend regionserver:0 has meta table
	c.metaRegionInfo.SetClient(rc)
	c.clients.put(rc, c.metaRegionInfo)

	ctx := context.Background()
	testTable := []byte("test")
	testKey := []byte("yolo")
	for i, tcase := range tcases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			c.regions.regions.Clear()
			// set up initial cache
			for _, region := range tcase.before {
				c.regions.put(region)
			}

			reg, err := c.findRegion(ctx, testTable, testKey)
			if err != tcase.err {
				t.Fatalf("Expected error %v, got %v", tcase.err, err)
			}
			if len(tcase.regName) == 0 {
				if reg != nil {
					t.Errorf("Expected nil region, got %v", reg)
				}
			} else if string(reg.Name()) != tcase.regName {
				t.Errorf("Expected region with name %q, got region %v",
					tcase.regName, reg)
			}

			// check cache
			if len(tcase.after) != c.regions.regions.Len() {
				t.Errorf("Expected to have %d regions in cache, got %d",
					len(tcase.after), c.regions.regions.Len())
			}
			for _, rn := range tcase.after {
				if _, ok := c.regions.regions.Get([]byte(rn)); !ok {
					t.Errorf("Expected region %q to be in regions cache", rn)
				}
			}
			// check client was looked up
			if tcase.establish {
				if ch := reg.AvailabilityChan(); ch != nil {
					// if still establishing, wait
					<-ch
				}
				rc2 := reg.Client()
				if rc2 == nil {
					t.Errorf("Expected region %q to establish a client", reg.Name())
					return
				}
				if rc2.Addr() != "regionserver:2" {
					t.Errorf("Expected regionserver:2, got %q", rc2.Addr())
				}
			}
		})
	}

}

func TestErrCannotFindRegion(t *testing.T) {
	c := newMockClient(nil)

	rc, err := region.NewClient(context.Background(), "regionserver:0",
		region.RegionClient, 0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}
	// pretend regionserver:0 has meta table
	c.metaRegionInfo.SetClient(rc)
	c.clients.put(rc, c.metaRegionInfo)

	// add young and small region to cache
	origlReg := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,yolo,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("yolo"), nil)
	c.regions.put(origlReg)
	c.clients.put(rc, origlReg)
	origlReg.SetClient(rc)

	// request a key not in the "yolo" region.
	get, err := hrpc.NewGetStr(context.Background(), "test", "meow")
	if err != nil {
		t.Fatal(err)
	}
	// it should lookup a new older region (1434573235908) that overlaps with the one in cache.
	// However, it shouldn't be put into cache, as it's older, resulting in a new lookup,
	// evetually leading to ErrCannotFindRegion.
	_, err = c.Get(get)
	if err != ErrCannotFindRegion {
		t.Errorf("Expected error %v, got error %v", ErrCannotFindRegion, err)
	}
}

func TestMetaLookupTableNotFound(t *testing.T) {
	c := newMockClient(nil)

	rc, err := region.NewClient(context.Background(), "regionserver:0",
		region.RegionClient, 0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}
	// pretend regionserver:0 has meta table
	c.metaRegionInfo.SetClient(rc)
	c.clients.put(rc, c.metaRegionInfo)

	_, _, err = c.metaLookup(context.Background(), []byte("tablenotfound"), []byte(t.Name()))
	if err != TableNotFound {
		t.Errorf("Expected error %v, got error %v", TableNotFound, err)
	}
}

func TestMetaLookupCanceledContext(t *testing.T) {
	c := newMockClient(nil)

	rc, err := region.NewClient(context.Background(), "regionserver:0",
		region.RegionClient, 0, 0, "root", region.DefaultReadTimeout)
	if err != nil {
		t.Fatal(err)
	}
	// pretend regionserver:0 has meta table
	c.metaRegionInfo.SetClient(rc)
	c.clients.put(rc, c.metaRegionInfo)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = c.metaLookup(ctx, []byte("tablenotfound"), []byte(t.Name()))
	if err != context.Canceled {
		t.Errorf("Expected error %v, got error %v", context.Canceled, err)
	}
}

func TestConcurrentRetryableError(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	zkc := mockZk.NewMockClient(ctrl)
	// keep failing on zookeeper lookup
	zkc.EXPECT().LocateResource(gomock.Any()).Return("", errors.New("ooops")).AnyTimes()
	c := newMockClient(zkc)
	// create region with mock clien
	origlReg := region.NewInfo(
		0,
		nil,
		[]byte("test1"),
		[]byte("test1,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		nil,
		nil,
	)
	// fake region to make sure we don't close the client
	whateverRegion := region.NewInfo(
		0,
		nil,
		[]byte("test2"),
		[]byte("test2,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		nil,
		nil,
	)
	rc := mockRegion.NewMockRegionClient(ctrl)
	rc.EXPECT().String().Return("mock region client").AnyTimes()
	rc.EXPECT().Addr().Return("host:1234").AnyTimes()
	origlReg.SetClient(rc)
	whateverRegion.SetClient(rc)
	c.regions.put(origlReg)
	c.regions.put(whateverRegion)
	c.clients.put(rc, origlReg)
	c.clients.put(rc, whateverRegion)

	numCalls := 100
	rc.EXPECT().QueueRPC(gomock.Any()).MinTimes(1)

	calls := make([]hrpc.Call, numCalls)
	for i := range calls {
		mockCall := mock.NewMockCall(ctrl)
		mockCall.EXPECT().SetRegion(origlReg).AnyTimes()
		mockCall.EXPECT().Context().Return(context.Background()).AnyTimes()
		result := make(chan hrpc.RPCResult, 1)
		result <- hrpc.RPCResult{Error: region.NotServingRegionError{}}
		mockCall.EXPECT().ResultChan().Return(result).AnyTimes()
		calls[i] = mockCall
	}

	var wg sync.WaitGroup
	for _, mockCall := range calls {
		wg.Add(1)
		go func(mockCall hrpc.Call) {
			_, err := c.sendRPCToRegion(mockCall, origlReg)
			if _, ok := err.(region.NotServingRegionError); !ok {
				t.Errorf("Got unexpected error: %v", err)
			}
			wg.Done()
		}(mockCall)
	}
	wg.Wait()
	origlReg.MarkDead()
	if ch := origlReg.AvailabilityChan(); ch != nil {
		<-ch
	}
}

func TestProbeKey(t *testing.T) {
	regions := []hrpc.RegionInfo{
		region.NewInfo(0, nil, nil, nil, nil, nil),
		region.NewInfo(0, nil, nil, nil, nil, []byte("yolo")),
		region.NewInfo(0, nil, nil, nil, []byte("swag"), nil),
		region.NewInfo(0, nil, nil, nil, []byte("swag"), []byte("yolo")),
		region.NewInfo(0, nil, nil, nil, []byte("aaaaaaaaaaaaaa"), []byte("bbbb")),
		region.NewInfo(0, nil, nil, nil, []byte("aaaa"), []byte("bbbbbbb")),
		region.NewInfo(0, nil, nil, nil, []byte("aaaa"), []byte("aab")),
		region.NewInfo(0, nil, nil, nil, []byte("aaa"), []byte("aaaaaaaaaa")),
		region.NewInfo(0, nil, nil, nil, []byte{255}, nil),
		region.NewInfo(0, nil, nil, nil, []byte{255, 255}, nil),
	}

	for _, reg := range regions {
		key := probeKey(reg)
		isGreaterThanStartOfTable := len(reg.StartKey()) == 0 && len(key) > 0
		isGreaterThanStartKey := bytes.Compare(reg.StartKey(), key) < 0
		isLessThanEndOfTable := len(reg.StopKey()) == 0 &&
			bytes.Compare(key, bytes.Repeat([]byte{255}, len(key))) < 0
		isLessThanStopKey := bytes.Compare(key, reg.StopKey()) < 0
		if (isGreaterThanStartOfTable || isGreaterThanStartKey) &&
			(isLessThanEndOfTable || isLessThanStopKey) {
			continue
		}
		t.Errorf("key %q is not within bounds of region %s: %v %v %v %v\n", key, reg,
			isGreaterThanStartOfTable, isGreaterThanStartKey,
			isLessThanEndOfTable, isLessThanStopKey)
	}
}
