// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build testing,!integration

package gohbase

import (
	"fmt"
	"testing"

	"github.com/aristanetworks/goarista/test"
	"github.com/cznic/b"
	"github.com/golang/mock/gomock"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/internal/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/test/mock"
	mockZk "github.com/tsuna/gohbase/test/mock/zk"
	"github.com/tsuna/gohbase/zk"
	"golang.org/x/net/context"
)

func newMockClient(zkClient zk.Client) *client {
	return &client{
		clientType: standardClient,
		regions:    keyRegionCache{regions: b.TreeNew(region.CompareGeneric)},
		clients: clientRegionCache{
			regions: make(map[hrpc.RegionClient][]hrpc.RegionInfo),
		},
		rpcQueueSize:  defaultRPCQueueSize,
		flushInterval: defaultFlushInterval,
		metaRegionInfo: region.NewInfo(
			[]byte("hbase:meta"),
			[]byte("hbase:meta,,1"),
			nil,
			nil),
		zkClient: zkClient,
	}
}

func TestSendRPCSanity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// we expect to ask zookeeper for where metaregion is
	zkClient := mockZk.NewMockClient(ctrl)
	zkClient.EXPECT().LocateResource(zk.Meta).Return(
		"regionserver", uint16(1), nil).MinTimes(1)
	c := newMockClient(zkClient)

	// ask for "theKey" in table "test"
	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().Context().Return(context.Background()).AnyTimes()
	mockCall.EXPECT().Table().Return([]byte("test")).AnyTimes()
	mockCall.EXPECT().Key().Return([]byte("theKey")).AnyTimes()
	mockCall.EXPECT().SetRegion(gomock.Any()).AnyTimes()
	result := make(chan hrpc.RPCResult, 1)
	// pretend that response is successful
	expMsg := &pb.GetResponse{}
	result <- hrpc.RPCResult{Msg: expMsg}
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	msg, err := c.sendRPC(mockCall)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if diff := test.Diff(expMsg, msg); diff != "" {
		t.Errorf("Expected: %#v\nReceived: %#v\nDiff:%s",
			expMsg, msg, diff)
	}

	if len(c.clients.regions) != 2 {
		t.Errorf("Expected 2 clients in cache, got %d", len(c.clients.regions))
	}

	// addr -> region name
	expClients := map[string]string{
		"regionserver:1": "hbase:meta,,1",
		"regionserver:2": "test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4.",
	}

	// make sure those are the right clients
	for c, r := range c.clients.regions {
		cAddr := fmt.Sprintf("%s:%d", c.Host(), c.Port())
		name, ok := expClients[cAddr]
		if !ok {
			t.Errorf("Got unexpected client %s:%d in cache", c.Host(), c.Port())
			continue
		}
		if len(r) != 1 {
			t.Errorf("Expected to have only 1 region in cache for client %s:%d",
				c.Host(), c.Port())
			continue
		}
		if string(r[0].Name()) != name {
			t.Errorf("Unexpected name of region %q for client %s:%d, expected %q",
				r[0].Name(), c.Host(), c.Port(), name)
		}
		// check bidirectional mapping, they have to be the same objects
		rc := r[0].Client()
		if c != rc {
			t.Errorf("Invalid bidirectional mapping: forward=%s:%d, backward=%s:%d",
				c.Host(), c.Port(), rc.Host(), rc.Port())
		}
	}

	if c.regions.regions.Len() != 1 {
		// expecting only one region because meta isn't in cache, it's hardcoded
		t.Errorf("Expected 1 regions in cache, got %d", c.regions.regions.Len())
	}
}
