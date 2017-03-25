// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/aristanetworks/goarista/test"
	"github.com/golang/mock/gomock"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/test/mock"
)

func cp(i uint64) *uint64 {
	j := i
	return &j
}

var resultsPB = []*pb.Result{
	// region 1
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("a"), Family: []byte("A"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("a"), Family: []byte("A"), Qualifier: []byte("2")},
			&pb.Cell{Row: []byte("a"), Family: []byte("B"), Qualifier: []byte("1")},
		},
		Exists: &tr,
	},
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("b"), Family: []byte("A"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("b"), Family: []byte("B"), Qualifier: []byte("2")},
		},
		Exists: &tr,
	},
	// region 2
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("bar"), Family: []byte("C"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("baz"), Family: []byte("C"), Qualifier: []byte("2")},
			&pb.Cell{Row: []byte("baz"), Family: []byte("C"), Qualifier: []byte("2")},
		},
		Exists: &tr,
	},
	// region 3
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("yolo"), Family: []byte("D"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("yolo"), Family: []byte("D"), Qualifier: []byte("2")},
		},
		Exists: &tr,
	},
}

var tr = true

func TestScanner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	ctx := context.Background()
	table := []byte("test")
	scan, err := hrpc.NewScan(ctx, table)
	if err != nil {
		t.Fatal(err)
	}

	var scannerID uint64 = 42

	scanner := newScanner(c, scan)
	ctx = scanner.f.ctx

	region1 := region.NewInfo(0, nil, table, []byte("table,,bar,whatever"), nil, []byte("bar"))
	s, err := hrpc.NewScanRange(ctx, table, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(s).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(&pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: &tr,
		Results:             resultsPB[:1],
	}, nil).Times(1)

	c.EXPECT().SendRPC(hrpc.NewScanFromID(ctx, table, scannerID, nil)).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(&pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   resultsPB[1:2],
	}, nil).Times(1)

	scannerID++

	region2 := region.NewInfo(0, nil, table,
		[]byte("table,bar,foo,whatever"), []byte("bar"), []byte("foo"))
	s, err = hrpc.NewScanRange(ctx, table, []byte("bar"), nil)
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(s).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region2)
	}).Return(&pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   resultsPB[2:3],
	}, nil).Times(1)

	scannerID++

	region3 := region.NewInfo(0, nil, table, []byte("table,foo,,whatever"), []byte("foo"), nil)

	s, err = hrpc.NewScanRange(ctx, table, []byte("foo"), nil)
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(s).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region3)
	}).Return(&pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   resultsPB[3:],
	}, nil).Times(1)

	var rs []*hrpc.Result
	for {
		r, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}

	var expected []*hrpc.Result
	for _, r := range resultsPB {
		expected = append(expected, hrpc.ToLocalResult(r))
	}

	if d := test.Diff(expected, rs); d != "" {
		t.Fatal(d)
	}
}

func TestScannerCloseBuffered(t *testing.T) {
	// test that if we close scanner when we still returning buffered results
	// send closing scan rpc
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	ctx := context.Background()
	table := []byte("test")
	scan, err := hrpc.NewScan(ctx, table)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	scanner := newScanner(c, scan)
	ctx = scanner.f.ctx
	var scannerID uint64 = 42

	r := region.NewInfo(0, nil, table, []byte("table,,,whatever"), nil, nil)
	s, err := hrpc.NewScanRange(ctx, table, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(s).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(r)
	}).Return(&pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: &tr,
		Results:             resultsPB[:2], // we got 2 rows
	}, nil).Times(1)

	// expect scan close rpc to be sent
	c.EXPECT().SendRPC(
		&scanMatcher{
			scan: hrpc.NewCloseFromID(context.Background(), table, scannerID, nil),
		}).
		Return(nil, nil).
		Times(1).
		Do(func(rpc hrpc.Call) { wg.Done() })

	g, err := scanner.Next()
	if err != nil {
		t.Fatal(err)
	}
	e := hrpc.ToLocalResult(resultsPB[0])
	if d := test.Diff(e, g); d != "" {
		t.Fatal(e, g, d)
	}

	scanner.Close()

	wg.Wait()
}

type scanMatcher struct {
	scan *hrpc.Scan
}

func (c *scanMatcher) Matches(x interface{}) bool {
	s, ok := x.(*hrpc.Scan)
	return ok &&
		s.IsClosing() == c.scan.IsClosing() &&
		bytes.Equal(s.Table(), c.scan.Table()) &&
		bytes.Equal(s.StartRow(), c.scan.StartRow())
}

func (c *scanMatcher) String() string {
	return fmt.Sprintf("is equal to Scan(table=%q closing=%v startrow=%q)",
		c.scan.Table(), c.scan.IsClosing(), c.scan.StartRow())
}
