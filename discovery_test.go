// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"testing"

	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/regioninfo"
	"golang.org/x/net/context"
)

func TestRegionDiscovery(t *testing.T) {
	client := NewClient("~invalid.quorum~") // We shouldn't connect to ZK.
	reg := client.getRegion([]byte("test"), []byte("theKey"))
	if reg != nil {
		t.Errorf("Found region %#v even though the cache was empty?!", reg)
	}

	// Stub out how we create new regions.
	savedNewRegion := newRegion
	defer func() { newRegion = savedNewRegion }()
	newRegion = func(res chan newRegResult, host string, port uint16) {
		res <- newRegResult{nil, nil}
	}

	// Inject a "test" table with a single region that covers the entire key
	// space (both the start and stop keys are empty).
	family := []byte("info")
	metaRow := &pb.GetResponse{
		Result: &pb.Result{Cell: []*pb.Cell{
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("regioninfo"),
				Value: []byte("PBUF\b\xc4\xcd\xe9\x99\xe0)\x12\x0f\n\adefault\x12\x04test" +
					"\x1a\x00\"\x00(\x000\x008\x00"),
			},
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("seqnumDuringOpen"),
				Value:     []byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
			},
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("server"),
				Value:     []byte("localhost:50966"),
			},
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("serverstartcode"),
				Value:     []byte("\x00\x00\x01N\x02\x92R\xb1"),
			},
		}}}

	_, _, err := client.discoverRegion(context.Background(), metaRow)
	if err != nil {
		t.Fatalf("Failed to discover region: %s", err)
	}

	reg = client.getRegion([]byte("test"), []byte("theKey"))
	if reg == nil {
		t.Fatal("Region not found even though we injected it in the cache.")
	}
	expected := &regioninfo.Info{
		Table:      []byte("test"),
		RegionName: []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		StartKey:   []byte(""),
		StopKey:    []byte(""),
	}
	if !bytes.Equal(reg.Table, expected.Table) ||
		!bytes.Equal(reg.RegionName, expected.RegionName) ||
		!bytes.Equal(reg.StartKey, expected.StartKey) ||
		!bytes.Equal(reg.StopKey, expected.StopKey) {
		t.Errorf("Found region %#v \nbut expected %#v", reg, expected)
	}

	reg = client.getRegion([]byte("notfound"), []byte("theKey"))
	if reg != nil {
		t.Errorf("Found region %#v even though this table doesn't exist", reg)
	}
}
