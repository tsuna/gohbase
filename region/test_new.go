// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build testing

package region

import (
	"bytes"
	"fmt"
	"time"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/internal/pb"
	"golang.org/x/net/context"
)

type testClient struct {
	host string
	port uint16
}

var clients = map[string]*testClient{
	"masterserver:1": &testClient{
		host: "masterserver",
		port: 1,
	},
	"regionserver:1": &testClient{
		host: "regionserver",
		port: 1,
	},
	"regionserver:2": &testClient{
		host: "regionserver",
		port: 2,
	},
	"regionserver:4": &testClient{
		host: "regionserver",
		port: 3,
	},
	"regionserver:5": &testClient{
		host: "regionserver",
		port: 4,
	},
}

var metaRow = &pb.Result{Cell: []*pb.Cell{
	&pb.Cell{
		Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		Family:    []byte("info"),
		Qualifier: []byte("regioninfo"),
		Value: []byte("PBUF\b\xc4\xcd\xe9\x99\xe0)\x12\x0f\n\adefault\x12\x04test" +
			"\x1a\x00\"\x00(\x000\x008\x00"),
	},
	&pb.Cell{
		Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		Family:    []byte("info"),
		Qualifier: []byte("seqnumDuringOpen"),
		Value:     []byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
	},
	&pb.Cell{
		Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		Family:    []byte("info"),
		Qualifier: []byte("server"),
		Value:     []byte("regionserver:2"),
	},
	&pb.Cell{
		Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		Family:    []byte("info"),
		Qualifier: []byte("serverstartcode"),
		Value:     []byte("\x00\x00\x01N\x02\x92R\xb1"),
	},
}}

// NewClient creates a new test region client.
func NewClient(ctx context.Context, host string, port uint16, ctype ClientType,
	queueSize int, flushInterval time.Duration) (hrpc.RegionClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	if c, ok := clients[addr]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("No client for %q", addr)
}

func (c *testClient) Host() string {
	return c.host
}

func (c *testClient) Port() uint16 {
	return c.port
}

func (c *testClient) QueueRPC(call hrpc.Call) {
	if bytes.Equal(call.Table(), []byte("hbase:meta")) {
		call.ResultChan() <- hrpc.RPCResult{&pb.GetResponse{Result: metaRow}, nil}
	}
}

func (c *testClient) Close() {}
