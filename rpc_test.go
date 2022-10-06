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
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsuna/gohbase/compression"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/test"
	"github.com/tsuna/gohbase/test/mock"
	mockRegion "github.com/tsuna/gohbase/test/mock/region"
	mockZk "github.com/tsuna/gohbase/test/mock/zk"
	"github.com/tsuna/gohbase/zk"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"modernc.org/b/v2"
)

func newRegionClientFn(addr string) func() hrpc.RegionClient {
	return func() hrpc.RegionClient {
		return newMockRegionClient(addr, region.RegionClient,
			0, 0, "root", region.DefaultReadTimeout, nil)
	}
}

func newMockClient(zkClient zk.Client) *client {
	return &client{
		clientType: region.RegionClient,
		regions:    keyRegionCache{regions: b.TreeNew[[]byte, hrpc.RegionInfo](region.Compare)},
		clients: clientRegionCache{
			regions: make(map[hrpc.RegionClient]map[hrpc.RegionInfo]struct{}),
		},
		rpcQueueSize:  defaultRPCQueueSize,
		flushInterval: defaultFlushInterval,
		metaRegionInfo: region.NewInfo(0, []byte("hbase"), []byte("meta"),
			[]byte("hbase:meta,,1"), nil, nil),
		zkTimeout:           defaultZkTimeout,
		zkClient:            zkClient,
		regionLookupTimeout: region.DefaultLookupTimeout,
		regionReadTimeout:   region.DefaultReadTimeout,
		newRegionClientFn:   newMockRegionClient,
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
	mockCall.EXPECT().Description().AnyTimes()
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
	if !proto.Equal(expMsg, msg) {
		t.Errorf("expected %v, got %v", expMsg, msg)
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

	// pretend regionserver:1 has meta table
	// "test1" is at the moment at regionserver:1
	// marking unavailable to simulate error
	origlReg.MarkUnavailable()
	rc1 := c.clients.put("regionserver:1", origlReg, newRegionClientFn("regionserver:1"))
	origlReg.SetClient(rc1)
	c.regions.put(origlReg)

	rc2 := c.clients.put("regionserver:1", c.metaRegionInfo, newRegionClientFn("regionserver:1"))
	if rc1 != rc2 {
		t.Fatal("expected to get the same region client")
	}
	c.metaRegionInfo.SetClient(rc2)

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
		if !reflect.DeepEqual(expRegs, gotRegs) {
			t.Errorf("expected %v, got %v", expRegs, gotRegs)
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

func TestReestablishRegionNSRE(t *testing.T) {
	c := newMockClient(nil)
	origlReg := region.NewInfo(0, nil, []byte("nsre"),
		[]byte("nsre,,1434573235908.56f833d5569a27c7a43fbf547b4924a4."), nil, nil)
	// inject a fake regionserver client and fake region into cache
	// pretend regionserver:1 has meta table
	rc1 := c.clients.put("regionserver:1", c.metaRegionInfo, newRegionClientFn("regionserver:1"))
	rc2 := c.clients.put("regionserver:1", origlReg, newRegionClientFn("regionserver:1"))
	if rc1 != rc2 {
		t.Fatal("expected region client to be the same")
	}

	// "nsre" is at the moment at regionserver:1
	c.metaRegionInfo.SetClient(rc1)
	origlReg.SetClient(rc1)
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

func TestEstablishRegionDialFail(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	c := newMockClient(nil)

	rcFailDial := mockRegion.NewMockRegionClient(ctrl)
	// return an error to make sure we lookup an new client
	rcFailDial.EXPECT().Dial(gomock.Any()).Return(errors.New("ooops")).AnyTimes()
	rcFailDial.EXPECT().Addr().Return("regionserver:1").AnyTimes()
	rcFailDial.EXPECT().String().Return("regionserver:1").AnyTimes()

	// second client returns that region has been dead
	// this is a success case to make sure we ever obtain a new client and not
	// just stuck looking up in cache
	rcDialCancel := mockRegion.NewMockRegionClient(ctrl)
	rcDialCancel.EXPECT().Dial(gomock.Any()).Return(context.Canceled)
	rcDialCancel.EXPECT().Addr().Return("regionserver:1").AnyTimes()
	rcDialCancel.EXPECT().String().Return("reginserver:1").AnyTimes()

	newRegionClientFnCallCount := 0
	c.newRegionClientFn = func(_ string, _ region.ClientType, _ int, _ time.Duration,
		_ string, _ time.Duration, _ compression.Codec) hrpc.RegionClient {
		var rc hrpc.RegionClient
		if newRegionClientFnCallCount == 0 {
			rc = rcFailDial
		} else {
			// if there was a bug with cache updates, we would never get into this case
			rc = rcDialCancel
		}
		newRegionClientFnCallCount++
		return rc
	}

	reg := region.NewInfo(
		0, nil, []byte("test1"), []byte("test1,,1434573235908.56f833d5569a27c7a43fbf547b4924a4."),
		nil, nil)
	reg.MarkUnavailable()

	// inject a fake regionserver client and fake region into cache
	// pretend regionserver:0 has meta table
	rc1 := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc1)

	// should get stuck if the region is never established
	c.establishRegion(reg, "regionserver:1")

	if len(c.clients.regions) != 2 {
		t.Errorf("Expected 2 clients in cache, got %d", len(c.clients.regions))
	}

	if reg.IsUnavailable() {
		t.Error("Expected region to be available")
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

	// pretend regionserver:0 has meta table
	rc := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc)

	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().Context().Return(context.Background()).AnyTimes()
	mockCall.EXPECT().Description().AnyTimes()
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

	if !proto.Equal(expMsg, msg) {
		t.Errorf("expected %v, got %v", expMsg, msg)
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
	c.regions.put(origlReg)
	rc := mockRegion.NewMockRegionClient(ctrl)
	rc.EXPECT().String().Return("mock region client").AnyTimes()
	c.clients.put("regionserver:0", origlReg, func() hrpc.RegionClient {
		return rc
	})
	origlReg.SetClient(rc)

	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().Region().Return(origlReg).Times(1)
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
		c.clients.put("regionserver:0", origlReg, func() hrpc.RegionClient {
			return rc2
		})
		origlReg.SetClient(rc2)

		// return ServerError from QueueRPC, to emulate dead client
		result <- hrpc.RPCResult{Error: region.ServerError{}}
	})

	_, err := c.sendRPCToRegionClient(context.Background(), mockCall, rc)
	switch err.(type) {
	case region.ServerError, region.NotServingRegionError:
	default:
		t.Errorf("Got unexpected error: %v", err)
	}

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
	// here we assume that this region was removed from
	// regions cache and thereby is considered dead
	reg := region.NewInfo(
		0,
		nil,
		[]byte("test"),
		[]byte("test,,1434573235908.yoloyoloyoloyoloyoloyoloyoloyolo."),
		nil, nil)
	reg.MarkDead()

	rc1 := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.clients.put("regionserver:0", reg, newRegionClientFn("regionserver:0"))

	// pretend regionserver:0 has meta table
	c.metaRegionInfo.SetClient(rc1)

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
	// pretend regionserver:0 has meta table
	rc := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc)

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

	// pretend regionserver:0 has meta table
	rc := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc)

	// add young and small region to cache
	origlReg := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,yolo,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("yolo"), nil)
	c.regions.put(origlReg)
	rc = c.clients.put("regionserver:0", origlReg, newRegionClientFn("regionserver:0"))
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
	// pretend regionserver:0 has meta table
	rc := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc)

	_, _, err := c.metaLookup(context.Background(), []byte("tablenotfound"), []byte(t.Name()))
	if err != TableNotFound {
		t.Errorf("Expected error %v, got error %v", TableNotFound, err)
	}
}

func TestMetaLookupCanceledContext(t *testing.T) {
	c := newMockClient(nil)
	// pretend regionserver:0 has meta table
	rc := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := c.metaLookup(ctx, []byte("tablenotfound"), []byte(t.Name()))
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
	newRC := func() hrpc.RegionClient {
		return rc
	}
	c.regions.put(origlReg)
	c.regions.put(whateverRegion)
	c.clients.put("host:1234", origlReg, newRC)
	c.clients.put("host:1234", whateverRegion, newRC)
	origlReg.SetClient(rc)
	whateverRegion.SetClient(rc)

	numCalls := 100
	rc.EXPECT().QueueRPC(gomock.Any()).MinTimes(1)

	calls := make([]hrpc.Call, numCalls)
	for i := range calls {
		mockCall := mock.NewMockCall(ctrl)
		mockCall.EXPECT().SetRegion(origlReg).AnyTimes()
		mockCall.EXPECT().Region().Return(origlReg).AnyTimes()
		result := make(chan hrpc.RPCResult, 1)
		result <- hrpc.RPCResult{Error: region.NotServingRegionError{}}
		mockCall.EXPECT().ResultChan().Return(result).AnyTimes()
		calls[i] = mockCall
	}

	var wg sync.WaitGroup
	for _, mockCall := range calls {
		wg.Add(1)
		go func(mockCall hrpc.Call) {
			_, err := c.sendRPCToRegionClient(context.Background(), mockCall, rc)
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

var (
	description = "GET"
	result      = "SUCCESS"
	o           prometheus.Observer
)

func BenchmarkPrometheusWithLabelValues(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		o = operationDurationSeconds.WithLabelValues(
			description,
			result,
		)
	}
}

func BenchmarkPrometheusWith(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		o = operationDurationSeconds.With(prometheus.Labels{
			"operation": description,
			"result":    result,
		})
	}
}

func TestSendBatchBasic(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	// we expect to ask zookeeper for where metaregion is
	zkClient := mockZk.NewMockClient(ctrl)
	zkClient.EXPECT().LocateResource(zk.Meta).Return("regionserver:1", nil).MinTimes(1)
	c := newMockClient(zkClient)

	call, err := hrpc.NewPutStr(context.Background(), "test", "theKey", nil)
	if err != nil {
		t.Fatal(err)
	}
	// pretend that response is successful
	expMsg := &pb.MutateResponse{}
	call.ResultChan() <- hrpc.RPCResult{Msg: expMsg}
	msg, ok := c.SendBatch(context.Background(), []hrpc.Call{call})
	if !ok {
		t.Fatal(msg[0].Error)
	}
	if !proto.Equal(expMsg, msg[0].Msg) {
		t.Errorf("expected %v, got %v", expMsg, msg)
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

func TestSendBatchBadInput(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	zkc := mockZk.NewMockClient(ctrl)
	zkc.EXPECT().LocateResource(zk.Meta).Return("regionserver:1", nil).AnyTimes()
	c := newMockClient(zkc)

	newRPC := func(table string, batchable bool) hrpc.Call {
		if batchable {
			rpc, err := hrpc.NewPutStr(context.Background(), table, "key",
				map[string]map[string][]byte{"cf": {"foo": []byte("bar")}})
			if err != nil {
				t.Fatal(err)
			}
			return rpc
		}
		rpc, err := hrpc.NewScanStr(context.Background(), table)
		if err != nil {
			t.Fatal(err)
		}
		return rpc
	}

	rpc1 := newRPC("table", true)
	rpc2 := newRPC("table", true)

	for _, tc := range []struct {
		name   string
		batch  []hrpc.Call
		expErr []string
	}{{
		name:   "duplicate",
		batch:  []hrpc.Call{rpc1, rpc1},
		expErr: []string{NotExecutedError.Error(), "duplicate call"},
	}, {
		name:  "duplicate2",
		batch: []hrpc.Call{rpc1, rpc2, rpc2, newRPC("table", true), rpc1},
		expErr: []string{NotExecutedError.Error(), NotExecutedError.Error(),
			"duplicate call", NotExecutedError.Error(), "duplicate call"},
	}, {
		name:   "tables",
		batch:  []hrpc.Call{newRPC("table", true), newRPC("different_table", true)},
		expErr: []string{NotExecutedError.Error(), "multiple tables"},
	}, {
		name:   "batchable",
		batch:  []hrpc.Call{newRPC("table", false)},
		expErr: []string{"non-batchable"},
	}, {
		name: "various errors",
		batch: []hrpc.Call{rpc1,
			newRPC("table", false),
			rpc1,
			newRPC("table2", true),
			newRPC("table", true)},
		expErr: []string{NotExecutedError.Error(),
			"non-batchable",
			"duplicate call",
			"multiple tables",
			NotExecutedError.Error()},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.batch) != len(tc.expErr) {
				t.Fatalf("test case provides mismatched batch (%d) and expErr (%d) sizes",
					len(tc.batch), len(tc.expErr))
			}

			results, ok := c.SendBatch(context.Background(), tc.batch)
			if ok {
				t.Errorf("expected !ok from SendBatch, got %t", ok)
			}
			if len(results) != len(tc.batch) {
				t.Fatalf("result size (%d) does not match batch size (%d)",
					len(results), len(tc.batch))
			}
			for i, res := range results {
				if res.Error == nil {
					t.Errorf("expected error in res[%d], but got nil for request %v",
						i, tc.batch[i])
					continue
				}
				if !strings.Contains(res.Error.Error(), tc.expErr[i]) {
					t.Errorf("expected error to contain %q, but got %q", tc.expErr[i], res.Error)
				}
			}
		})
	}
}

// TestFindClient ensures findClients groups RPCs in a batch by region
// server and preserves the ordering of requests. And that each RPC
// has its region assigned.
func TestFindClients(t *testing.T) {
	c := newMockClient(nil)
	// pretend regionserver:0 has meta table
	rc := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc)

	registerRegion := func(reg hrpc.RegionInfo, addr string) {
		rc := c.clients.put(addr, reg, newRegionClientFn(addr))
		reg.SetClient(rc)
		overlaps, replaced := c.regions.put(reg)
		if len(overlaps) > 0 {
			t.Fatalf("overlaps: %v replaced: %t", overlaps, replaced)
		}
	}
	// Create three region servers, each with three regions
	regA := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,a,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("a"), []byte("az"))
	regB := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,b,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("b"), []byte("bz"))
	regC := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,c,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("c"), []byte("cz"))
	for _, reg := range []hrpc.RegionInfo{regA, regB, regC} {
		registerRegion(reg, "regionserver:0")
	}
	regD := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,d,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("d"), []byte("dz"))
	regE := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,e,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("e"), []byte("ez"))
	regF := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,f,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("f"), []byte("fz"))
	for _, reg := range []hrpc.RegionInfo{regD, regE, regF} {
		registerRegion(reg, "regionserver:1")
	}
	regG := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,g,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("g"), []byte("gz"))
	regH := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,h,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("h"), []byte("hz"))
	regI := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,i,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("i"), []byte("iz"))
	for _, reg := range []hrpc.RegionInfo{regG, regH, regI} {
		registerRegion(reg, "regionserver:2")
	}

	if l := len(c.clients.regions); l != 3 {
		t.Errorf("expected 3 region clients, got %d", l)
		t.Logf("%v", c.clients.regions)
	}

	if l := c.regions.regions.Len(); l != 9 {
		t.Errorf("expected 9 regions, got %d", l)
	}

	newRPC := func(key string) hrpc.Call {
		rpc, err := hrpc.NewPutStr(context.Background(), "test", key,
			map[string]map[string][]byte{"cf": {"foo": []byte("bar")}})
		if err != nil {
			t.Fatal(err)
		}
		return rpc
	}

	type kr struct {
		key string
		reg hrpc.RegionInfo
	}

	type testCase struct {
		name    string
		batch   []hrpc.Call
		exp     map[string][]kr
		expErrs []string
	}

	for _, tc := range []testCase{{
		name: "one_rc",
		batch: []hrpc.Call{
			newRPC("aa"),
			newRPC("ba"),
			newRPC("ca"),
		},
		exp: map[string][]kr{
			"regionserver:0": {
				{"aa", regA},
				{"ba", regB},
				{"ca", regC},
			},
		},
	}, {
		name: "three_rc",
		batch: []hrpc.Call{
			newRPC("aa"),
			newRPC("da"),
			newRPC("ga"),
			newRPC("ab"),
		},
		exp: map[string][]kr{
			"regionserver:0": {
				{"aa", regA},
				{"ab", regA},
			},
			"regionserver:1": {
				{"da", regD},
			},
			"regionserver:2": {
				{"ga", regG},
			},
		},
	}, {
		name: "error end",
		batch: []hrpc.Call{
			newRPC("aa"),
			newRPC("da"),
			newRPC("ga"),
			newRPC("zz"), // missing region
		},
		expErrs: []string{"", "", "", "cannot find region"},
	}, {
		name: "error middle",
		batch: []hrpc.Call{
			newRPC("aa"),
			newRPC("da"),
			newRPC("zz"), // missing region
			newRPC("ga"),
		},
		expErrs: []string{"", "", "cannot find region", ""},
	}, {
		name: "error beginning",
		batch: []hrpc.Call{
			newRPC("zz"), // missing region
			newRPC("aa"),
			newRPC("da"),
			newRPC("ga"),
		},
		expErrs: []string{"cannot find region", "", "", ""},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			results := make([]hrpc.RPCResult, len(tc.batch))
			got, ok := c.findClients(context.Background(), tc.batch, results)
			if ok && len(tc.expErrs) > 0 {
				t.Fatalf("expected error, %v", results[3])

			} else if !ok && len(tc.expErrs) == 0 {
				t.Fatalf("unexpected !ok: %v", results)
			} else if !ok {
				for i, res := range results {
					expErr := tc.expErrs[i]
					if expErr == "" && res.Error != nil {
						t.Errorf("unexpected error: %v", res.Error)
					}
					if expErr == "" {
						continue
					}
					if !strings.Contains(res.Error.Error(), expErr) {
						t.Errorf("expected error to contain %q, got: %v", expErr, res.Error)
					}
				}
				return
			}

			for rc, rpcs := range got {
				expRPCs, ok := tc.exp[rc.Addr()]
				if !ok {
					t.Errorf("unexpected region client: %q", rc.Addr())
				}
				delete(tc.exp, rc.Addr())
				for i, rpc := range rpcs {
					if i >= len(expRPCs) {
						t.Errorf("unexpected RPC for region client %q: %s",
							rc.Addr(), rpc.Key())
						continue
					}
					expRPC := expRPCs[i]
					if string(rpc.Key()) != expRPC.key {
						t.Errorf("for region client %q expected rpc %s, but got %s",
							rc.Addr(), expRPC.key, rpc.Key())
					}
					if rpc.Region() != expRPC.reg {
						t.Errorf("for region client %q rpc %s, expected reg %v but got %v",
							rc.Addr(), expRPC.key, rpc.Region(), expRPC.reg)
					}
				}
				if len(expRPCs) > len(rpcs) {
					for _, rpc := range expRPCs[len(rpcs):] {
						t.Errorf("expected RPC for region client %q: %s",
							rc.Addr(), rpc.key)
					}
				}
			}
			for rc, rpcs := range tc.exp {
				t.Errorf("expected rpcs for region client %q: %v", rc, rpcs)
			}
		})
	}
}

func TestSendBatchWaitForCompletion(t *testing.T) {
	c := newMockClient(nil)
	// pretend regionserver:0 has meta table
	rc := c.clients.put("regionserver:0", c.metaRegionInfo, newRegionClientFn("regionserver:0"))
	c.metaRegionInfo.SetClient(rc)

	registerRegion := func(reg hrpc.RegionInfo, addr string) {
		rc := c.clients.put(addr, reg, newRegionClientFn(addr))
		reg.SetClient(rc)
		overlaps, replaced := c.regions.put(reg)
		if len(overlaps) > 0 {
			t.Fatalf("overlaps: %v replaced: %t", overlaps, replaced)
		}
	}
	// Create three region servers, each with three regions
	regA := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,a,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("a"), []byte("az"))
	regB := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,b,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("b"), []byte("bz"))
	regC := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,c,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("c"), []byte("cz"))
	for _, reg := range []hrpc.RegionInfo{regA, regB, regC} {
		registerRegion(reg, "regionserver:0")
	}
	regD := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,d,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("d"), []byte("dz"))
	regE := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,e,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("e"), []byte("ez"))
	regF := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,f,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("f"), []byte("fz"))
	for _, reg := range []hrpc.RegionInfo{regD, regE, regF} {
		registerRegion(reg, "regionserver:1")
	}
	regG := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,g,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("g"), []byte("gz"))
	regH := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,h,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("h"), []byte("hz"))
	regI := region.NewInfo(1434573235910, nil, []byte("test"),
		[]byte("test,i,1434573235910.56f833d5569a27c7a43fbf547b4924a4."), []byte("i"), []byte("iz"))
	for _, reg := range []hrpc.RegionInfo{regG, regH, regI} {
		registerRegion(reg, "regionserver:2")
	}

	if l := len(c.clients.regions); l != 3 {
		t.Errorf("expected 3 region clients, got %d", l)
		t.Logf("%v", c.clients.regions)
	}

	if l := c.regions.regions.Len(); l != 9 {
		t.Errorf("expected 9 regions, got %d", l)
	}

	newRPC := func(key string) hrpc.Call {
		rpc, err := hrpc.NewPutStr(context.Background(), "test", key,
			map[string]map[string][]byte{"cf": {"foo": []byte("bar")}})
		if err != nil {
			t.Fatal(err)
		}
		return rpc
	}

	t.Run("success", func(t *testing.T) {
		batch := make([]hrpc.Call, 9)
		for i := 0; i < 9; i++ {
			// Create an RPC for each region, "a"-"i"
			batch[i] = newRPC(string(rune('a' + i)))
		}
		var (
			result []hrpc.RPCResult
			ok     bool
			done   = make(chan struct{})
		)
		go func() {
			result, ok = c.SendBatch(context.Background(), batch)
			close(done)
		}()
		for i := 0; i < 9; i++ {
			// Using an Int32 as a result. A real result would be a
			// PutResponse, but any proto.Message works for the test.
			batch[i].ResultChan() <- hrpc.RPCResult{Msg: wrapperspb.Int32(int32(i))}
		}
		<-done
		if !ok {
			t.Errorf("unexpected !ok")
		}
		if len(result) != 9 {
			t.Fatalf("unexpected result size: %v", result)
		}
		for i, r := range result {
			if r.Error != nil {
				t.Errorf("unexpected error: %s", r.Error)
				continue
			}
			if r.Msg.(*wrapperspb.Int32Value).Value != int32(i) {
				t.Errorf("unexpected result: %v", r.Msg)
			}
		}
	})

	t.Run("error all", func(t *testing.T) {
		batch := make([]hrpc.Call, 9)
		for i := 0; i < 9; i++ {
			// Create an RPC for each region, "a"-"i"
			key := string(rune('a' + i))
			batch[i] = newRPC(key)
		}
		var (
			result []hrpc.RPCResult
			ok     bool
			done   = make(chan struct{})
		)
		go func() {
			result, ok = c.SendBatch(context.Background(), batch)
			close(done)
		}()
		for i := 0; i < 9; i++ {
			batch[i].ResultChan() <- hrpc.RPCResult{Error: errors.New("error")}
		}
		<-done
		if ok {
			t.Errorf("expected !ok")
		}

		if len(result) != 9 {
			t.Fatalf("unexpected result size: %v", result)
		}
		for _, r := range result {
			if r.Error == nil || r.Error.Error() != "error" {
				t.Errorf("expected error but got: %v", r.Error)
			}
			if r.Msg != nil {
				t.Errorf("unexpected Msg: %v", r.Msg)
			}
			continue
		}
	})

	t.Run("error one", func(t *testing.T) {
		batch := make([]hrpc.Call, 9)
		for i := 0; i < 9; i++ {
			// Create an RPC for each region, "a"-"i"
			key := string(rune('a' + i))
			batch[i] = newRPC(key)
		}
		var (
			result []hrpc.RPCResult
			ok     bool
			done   = make(chan struct{})
		)
		go func() {
			result, ok = c.SendBatch(context.Background(), batch)
			close(done)
		}()
		errIndex := rand.Intn(9)
		for i := 0; i < 9; i++ {
			if i == errIndex {
				batch[i].ResultChan() <- hrpc.RPCResult{Error: errors.New("error")}
				continue
			}
			// Using an Int32 as a result. A real result would be a
			// PutResponse, but any proto.Message works for the test.
			batch[i].ResultChan() <- hrpc.RPCResult{Msg: wrapperspb.Int32(int32(i))}
		}
		<-done
		if ok {
			t.Errorf("expected !ok")
		}
		if len(result) != 9 {
			t.Fatalf("unexpected result size: %v", result)
		}
		for i, r := range result {
			if i == errIndex {
				if r.Error == nil || r.Error.Error() != "error" {
					t.Errorf("expected error but got: %v", r.Error)
				}
				if r.Msg != nil {
					t.Errorf("unexpected Msg: %v", r.Msg)
				}
				continue
			}
			if r.Error != nil {
				t.Errorf("unexpected error: %s", r.Error)
				continue
			}
			if r.Msg.(*wrapperspb.Int32Value).Value != int32(i) {
				t.Errorf("unexpected result: %v", r.Msg)
			}
		}
	})

	t.Run("error some", func(t *testing.T) {
		batch := make([]hrpc.Call, 9)
		for i := 0; i < 9; i++ {
			// Create an RPC for each region, "a"-"i"
			key := string(rune('a' + i))
			batch[i] = newRPC(key)
		}
		var (
			result []hrpc.RPCResult
			ok     bool
			done   = make(chan struct{})
		)
		go func() {
			result, ok = c.SendBatch(context.Background(), batch)
			close(done)
		}()

		// Error on RPCs 0 (first RPC to a region server), 4 (middle
		// RPC to a region server), 8 (last to a region server)
		for i := 0; i < 9; i++ {
			if i%4 == 0 {
				batch[i].ResultChan() <- hrpc.RPCResult{Error: errors.New("error")}
				continue
			}
			// Using an Int32 as a result. A real result would be a
			// PutResponse, but any proto.Message works for the test.
			batch[i].ResultChan() <- hrpc.RPCResult{Msg: wrapperspb.Int32(int32(i))}
		}
		<-done
		if ok {
			t.Errorf("expected !ok")
		}
		if len(result) != 9 {
			t.Fatalf("unexpected result size: %v", result)
		}
		for i, r := range result {
			if i%4 == 0 {
				if r.Error == nil || r.Error.Error() != "error" {
					t.Errorf("expected error but got: %v", r.Error)
				}
				if r.Msg != nil {
					t.Errorf("unexpected Msg: %v", r.Msg)
				}
				continue
			}
			if r.Error != nil {
				t.Errorf("unexpected error: %s", r.Error)
				continue
			}
			if r.Msg.(*wrapperspb.Int32Value).Value != int32(i) {
				t.Errorf("unexpected result: %v", r.Msg)
			}
		}
	})

	t.Run("cancel context all", func(t *testing.T) {
		batch := make([]hrpc.Call, 9)
		for i := 0; i < 9; i++ {
			// Create an RPC for each region, "a"-"i"
			key := string(rune('a' + i))
			batch[i] = newRPC(key)
		}
		ctx, cancel := context.WithCancel(context.Background())
		var (
			result []hrpc.RPCResult
			ok     bool
			done   = make(chan struct{})
		)
		go func() {
			result, ok = c.SendBatch(ctx, batch)
			close(done)
		}()
		cancel()
		<-done
		if ok {
			t.Errorf("expected !ok")
		}
		if len(result) != 9 {
			t.Fatalf("unexpected result size: %v", result)
		}
		for _, r := range result {
			if r.Error == nil ||
				!errors.Is(r.Error, context.Canceled) {
				t.Errorf("expected canceled error but got: %v", r.Error)
			}
			if r.Msg != nil {
				t.Errorf("unexpected Msg: %v", r.Msg)
			}
		}
	})

	t.Run("cancel context some", func(t *testing.T) {
		batch := make([]hrpc.Call, 9)
		for i := 0; i < 9; i++ {
			// Create an RPC for each region, "a"-"i"
			key := string(rune('a' + i))
			batch[i] = newRPC(key)
		}
		ctx, cancel := context.WithCancel(context.Background())
		var (
			result []hrpc.RPCResult
			ok     bool
			done   = make(chan struct{})
		)
		go func() {
			result, ok = c.SendBatch(ctx, batch)
			close(done)
		}()
		// Send results on RPCs except for 0 (first RPC to a region
		// server), 4 (middle RPC to a region server), 8 (last to a
		// region server).
		for i := 0; i < 9; i++ {
			if i%4 == 0 {
				continue
			}
			// Error some responses
			if i%2 == 0 {
				batch[i].ResultChan() <- hrpc.RPCResult{Error: errors.New("error")}
				continue
			}
			// Using an Int32 as a result. A real result would be a
			// MutateResponse, but any proto.Message works for the test.
			batch[i].ResultChan() <- hrpc.RPCResult{Msg: wrapperspb.Int32(int32(i))}
		}
		// Cancel the context. The completed RPCs, should still get
		// their results.
		cancel()
		<-done
		if ok {
			t.Errorf("expected !ok")
		}
		if len(result) != 9 {
			t.Fatalf("unexpected result size: %v", result)
		}
		for i, r := range result {
			if i%2 == 0 {
				expErrStr := "error"
				if i%4 == 0 {
					expErrStr = context.Canceled.Error()
				}
				if r.Error == nil ||
					r.Error.Error() != expErrStr {
					t.Errorf("expected canceled error but got: %v", r.Error)
				}
				if r.Msg != nil {
					t.Errorf("unexpected Msg: %v", r.Msg)
				}
				continue
			}
			if r.Error != nil {
				t.Errorf("unexpected error: %s", r.Error)
				continue
			}
			if r.Msg.(*wrapperspb.Int32Value).Value != int32(i) {
				t.Errorf("unexpected result: %v", r.Msg)
			}
		}
	})
}
