// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/test"
	"github.com/tsuna/gohbase/test/mock"
	"google.golang.org/protobuf/proto"
)

func TestScannerV2(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)
	var expected []*hrpc.ScanResponseV2

	scan, err := hrpc.NewScan(context.Background(), table, hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}

	var scannerID uint64 = 42
	scanner := newScannerV2(c, scan, slog.Default())

	s, err := hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}
	resp1 := &pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: proto.Bool(true),
		Results:             dup(resultsPB[:1]),
	}
	expected = append(expected, pbRespToRespV2(resp1))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp1)
			return resp1, nil
		}).Times(1)

	s, err = hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.ScannerID(scannerID), hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}

	resp2 := &pb.ScanResponse{
		Results: dup(resultsPB[1:2]),
	}
	expected = append(expected, pbRespToRespV2(resp2))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp2)
			return resp2, nil
		}).Times(1)

	scannerID++

	s, err = hrpc.NewScanRange(scan.Context(), table,
		[]byte("bar"), nil, hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}
	resp3 := &pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   dup(resultsPB[2:3]),
	}
	expected = append(expected, pbRespToRespV2(resp3))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region2)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp3)
			return resp3, nil
		}).Times(1)

	scannerID++

	s, err = hrpc.NewScanRange(scan.Context(), table, []byte("foo"), nil,
		hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}
	resp4 := &pb.ScanResponse{
		ScannerId:   cp(scannerID),
		Results:     dup(resultsPB[3:4]),
		MoreResults: proto.Bool(false),
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region3)

			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp4)
			return resp4, nil
		}).Times(1)

	expected = append(expected, pbRespToRespV2(resp4))

	var rs []*hrpc.ScanResponseV2
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

	if !cmp.Equal(expected, rs) {
		t.Fatalf("exp: %v\ngot: %v", expected, rs)
	}
}

func TestScanMetricsV2(t *testing.T) {
	cellsToResponse := func(cells ...[]*pb.Cell) []*hrpc.ScanResponseV2 {
		var resp []*hrpc.ScanResponseV2
		for _, c := range cells {
			resp = append(resp, pbRespToRespV2(&pb.ScanResponse{Results: []*pb.Result{{Cell: c}}}))
		}
		return resp
	}

	scanned, filtered := rowsScanned, rowsFiltered
	i0, i1, i2, i4 := int64(0), int64(1), int64(2), int64(4)
	tcases := []struct {
		description          string
		trackScanMetrics     func(call hrpc.Call) error
		filter               func(call hrpc.Call) error
		results              []*pb.Result
		scanMetrics          *pb.ScanMetrics
		expectedResults      []*hrpc.ScanResponseV2
		expectedRowsScanned  int64
		expectedRowsFiltered int64
	}{
		{
			description: "ScanMetrics not enabled",
			results: []*pb.Result{
				{Cell: cells[:3]},
			},
			scanMetrics:     nil,
			expectedResults: cellsToResponse(cells[:3]),
		},
		{
			description:          "Empty results",
			trackScanMetrics:     hrpc.TrackScanMetrics(),
			results:              nil,
			scanMetrics:          nil,
			expectedResults:      nil,
			expectedRowsScanned:  0,
			expectedRowsFiltered: 0,
		},
		{
			description:      "ScanMetrics: 1 row scanned",
			trackScanMetrics: hrpc.TrackScanMetrics(),
			results: []*pb.Result{
				{Cell: cells[:3]},
			},
			scanMetrics: &pb.ScanMetrics{
				Metrics: []*pb.NameInt64Pair{
					{
						Name:  &scanned,
						Value: &i1,
					},
					{
						Name:  &filtered,
						Value: &i0,
					},
				},
			},
			expectedResults:      cellsToResponse(cells[:3]),
			expectedRowsScanned:  1,
			expectedRowsFiltered: 0,
		},
		{
			description:      "ScanMetrics: 2 rows scanned",
			trackScanMetrics: hrpc.TrackScanMetrics(),
			results: []*pb.Result{
				{Cell: cells[:5]},
			},
			scanMetrics: &pb.ScanMetrics{
				Metrics: []*pb.NameInt64Pair{
					{
						Name:  &scanned,
						Value: &i2,
					},
					{
						Name:  &filtered,
						Value: &i0,
					},
				},
			},
			expectedResults:      cellsToResponse(cells[:5]),
			expectedRowsScanned:  2,
			expectedRowsFiltered: 0,
		},
		{
			description:      "ScanMetrics: 4 rows scanned, 2 row filtered",
			trackScanMetrics: hrpc.TrackScanMetrics(),
			filter:           hrpc.Filters(filter.NewPrefixFilter([]byte("b"))),
			results: []*pb.Result{
				{Cell: cells},
			},
			scanMetrics: &pb.ScanMetrics{
				Metrics: []*pb.NameInt64Pair{
					{
						Name:  &scanned,
						Value: &i4,
					},
					{
						Name:  &filtered,
						Value: &i2,
					},
				},
			},
			expectedResults:      cellsToResponse(cells),
			expectedRowsScanned:  4,
			expectedRowsFiltered: 2,
		},
		{
			description:      "ScanMetrics: 2 rows scanned, 1 row filtered",
			trackScanMetrics: hrpc.TrackScanMetrics(),
			filter:           hrpc.Filters(filter.NewPrefixFilter([]byte("a"))),
			results: []*pb.Result{
				{Cell: cells[:5]},
			},
			scanMetrics: &pb.ScanMetrics{
				Metrics: []*pb.NameInt64Pair{
					{
						Name:  &scanned,
						Value: &i2,
					},
					{
						Name:  &filtered,
						Value: &i1,
					},
				},
			},
			expectedResults:      cellsToResponse(cells[:5]),
			expectedRowsScanned:  2,
			expectedRowsFiltered: 1,
		},
		{
			description:      "ScanMetrics: 0 rows scanned, 1 row filtered",
			trackScanMetrics: hrpc.TrackScanMetrics(),
			filter:           hrpc.Filters(filter.NewPrefixFilter([]byte("a"))),
			results: []*pb.Result{
				{Cell: cells[:3]},
			},
			scanMetrics: &pb.ScanMetrics{
				Metrics: []*pb.NameInt64Pair{
					{
						Name:  &scanned,
						Value: &i0,
					},
					{
						Name:  &filtered,
						Value: &i1,
					},
				},
			},
			expectedResults:      cellsToResponse(cells[:3]),
			expectedRowsScanned:  0,
			expectedRowsFiltered: 1,
		},
	}

	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	for _, tcase := range tcases {
		t.Run(tcase.description, func(t *testing.T) {
			ctx := context.Background()
			var scan *hrpc.Scan
			var err error
			if tcase.trackScanMetrics != nil && tcase.filter != nil {
				scan, err = hrpc.NewScan(ctx, table, tcase.trackScanMetrics, tcase.filter)
			} else if tcase.trackScanMetrics != nil {
				scan, err = hrpc.NewScan(ctx, table, tcase.trackScanMetrics)
			} else {
				scan, err = hrpc.NewScan(ctx, table)
			}

			if err != nil {
				t.Fatal(err)
			}

			sc := newScannerV2(c, scan, slog.Default())

			c.EXPECT().SendRPC(&scanMatcher{scan: scan}).DoAndReturn(
				func(rpc hrpc.Call) (msg proto.Message, err error) {
					resp := &pb.ScanResponse{
						Results:     tcase.results,
						ScanMetrics: tcase.scanMetrics,
					}
					rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp)
					return resp, nil
				}).Times(1)

			var res []*hrpc.ScanResponseV2
			for {
				r, err := sc.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				res = append(res, r)
			}

			actualMetrics := sc.GetScanMetrics()

			if tcase.trackScanMetrics == nil && actualMetrics != nil {
				t.Fatalf("Got non-nil scan metrics when not enabled: %v", actualMetrics)
			}

			if tcase.expectedRowsScanned != actualMetrics[rowsScanned] {
				t.Errorf("Did not get expected rows scanned - expected: %d, actual %d",
					tcase.expectedRowsScanned, actualMetrics[rowsScanned])
			}

			if tcase.expectedRowsFiltered != actualMetrics[rowsFiltered] {
				t.Errorf("Did not get expected rows filtered - expected: %d, actual %d",
					tcase.expectedRowsFiltered, actualMetrics[rowsFiltered])
			}

			if !cmp.Equal(tcase.expectedResults, res) {
				t.Fatalf("expected: %+v\ngot: %+v", tcase.expectedResults, res)
			}
		})
	}
}

func TestErrorScanFromIDV2(t *testing.T) {
	scan, err := hrpc.NewScan(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	expected := []*hrpc.Result{
		hrpc.ToLocalResult(&pb.Result{Cell: cells[:3]}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[3:4]}),
	}
	testErrorScanFromID(t, scan, expected)
}

func TestErrorFirstFetchNoMetricsV2(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	scan, err := hrpc.NewScan(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	scanner := newScannerV2(c, scan, slog.Default())

	srange, err := hrpc.NewScanRange(context.Background(), table, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	outErr := errors.New("WTF")
	c.EXPECT().SendRPC(&scanMatcher{scan: srange}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(nil, outErr).Times(1)

	r, err := scanner.Next()
	if err != outErr {
		t.Errorf("Expected error %v, got error %v", outErr, err)
	}
	if r != nil {
		t.Fatalf("expected no results, got %v", r)
	}
}

func TestErrorFirstFetchWithMetricsV2(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	scan, err := hrpc.NewScan(context.Background(), table, hrpc.TrackScanMetrics())
	if err != nil {
		t.Fatal(err)
	}
	scanner := newScannerV2(c, scan, slog.Default())

	srange, err := hrpc.NewScanRange(context.Background(), table, nil, nil,
		hrpc.TrackScanMetrics())
	if err != nil {
		t.Fatal(err)
	}

	outErr := errors.New("WTF")
	c.EXPECT().SendRPC(&scanMatcher{scan: srange}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(nil, outErr).Times(1)

	r, err := scanner.Next()
	if err != outErr {
		t.Errorf("Expected error %v, got error %v", outErr, err)
	}
	if r != nil {
		t.Fatalf("expected no results, got %v", r)
	}
}

func testErrorScanFromIDV2(t *testing.T, scan *hrpc.Scan, out []*hrpc.Result) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	var scannerID uint64 = 42
	scanner := newScannerV2(c, scan, slog.Default())

	srange, err := hrpc.NewScanRange(scan.Context(), table, nil, nil, scan.Options()...)
	if err != nil {
		t.Fatal(err)
	}

	resp := &pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: proto.Bool(true),
		Results: []*pb.Result{
			&pb.Result{Cell: cells[:3]},
			&pb.Result{Cell: cells[3:4]},
		},
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: srange}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp)
			return resp, nil
		}).Times(1)

	outErr := errors.New("WTF")

	sid, err := hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.ScannerID(scannerID))
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: sid}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(nil, outErr).Times(1)

	// expect scan close rpc to be sent
	testCallClose(sid, c, scannerID, &wg, t)

	var rs []*hrpc.ScanResponseV2
	for {
		r, err := scanner.Next()
		if r != nil {
			rs = append(rs, r)
		}
		if err != nil {
			break
		}
	}

	if err != outErr {
		t.Errorf("Expected error %v, got error %v", outErr, err)
	}
	if !reflect.DeepEqual(out, rs) {
		t.Fatalf("expected %v, got %v", out, rs)
	}
}

func TestReversedScannerV2(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	var expected []*hrpc.ScanResponseV2

	ctx := context.Background()
	scan, err := hrpc.NewScan(ctx, table, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}

	var scannerID uint64 = 42

	scanner := newScannerV2(c, scan, slog.Default())
	ctx = scan.Context()
	s, err := hrpc.NewScanRange(ctx, table, nil, nil, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}
	resp1 := &pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   dup(resultsPB[3:4]),
	}
	expected = append(expected, pbRespToRespV2(resp1))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region3)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp1)
			return resp1, nil
		}).Times(1)

	s, err = hrpc.NewScanRange(ctx, table,
		append([]byte("fon"), rowPadding...), nil, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}
	resp2 := &pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   dup(resultsPB[2:3]),
	}
	expected = append(expected, pbRespToRespV2(resp2))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region2)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp2)
			return resp2, nil
		}).Times(1)

	s, err = hrpc.NewScanRange(ctx, table,
		append([]byte("baq"), rowPadding...), nil, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}
	resp3 := &pb.ScanResponse{
		MoreResultsInRegion: proto.Bool(true),
		ScannerId:           cp(scannerID),
		Results:             dup(resultsPB[1:2]),
	}
	expected = append(expected, pbRespToRespV2(resp3))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp3)
			return resp3, nil
		}).Times(1)

	s, err = hrpc.NewScanRange(ctx, table, nil, nil, hrpc.ScannerID(scannerID))
	if err != nil {
		t.Fatal(err)
	}
	resp4 := &pb.ScanResponse{
		Results: dup(resultsPB[:1]),
	}
	expected = append(expected, pbRespToRespV2(resp4))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp4)
			return resp4, nil
		}).Times(1)

	var rs []*hrpc.ScanResponseV2
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

	if !cmp.Equal(expected, rs) {
		t.Fatalf("expected %v, got %v", expected, rs)
	}
}

func TestScannerV2WithContextCanceled(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	scan, err := hrpc.NewScan(ctx, []byte(t.Name()))
	if err != nil {
		t.Fatal(err)
	}

	scanner := newScannerV2(c, scan, slog.Default())

	cancel()

	_, err = scanner.Next()
	if err != context.Canceled {
		t.Fatalf("unexpected error %v, expected %v", err, context.Canceled)
	}
}

func TestScannerV2Closed(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	scan, err := hrpc.NewScan(context.Background(), []byte(t.Name()))
	if err != nil {
		t.Fatal(err)
	}

	scanner := newScannerV2(c, scan, slog.Default())
	scanner.Close()

	_, err = scanner.Next()
	if err != io.EOF {
		t.Fatalf("unexpected error %v, expected %v", err, io.EOF)
	}
}

func TestScannerV2Scan(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)
	var expected []*hrpc.ScanResponseV2

	scan, err := hrpc.NewScan(context.Background(), table, hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}

	var scannerID uint64 = 42
	scanner := newScannerV2(c, scan, slog.Default())

	// First scan request - returns results with MoreResultsInRegion=true
	s, err := hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.NumberOfRows(3))
	if err != nil {
		t.Fatal(err)
	}
	resp1 := &pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: proto.Bool(true),
		Results:             dup(resultsPB[:1]),
	}
	expected = append(expected, pbRespToRespV2(resp1))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp1)
			return resp1, nil
		}).Times(1)

	// Second scan with scanner ID - returns empty result (scanner timeout case)
	s, err = hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.ScannerID(scannerID), hrpc.NumberOfRows(3))
	if err != nil {
		t.Fatal(err)
	}
	resp2 := &pb.ScanResponse{
		MoreResultsInRegion: proto.Bool(true),
	}
	expected = append(expected, pbRespToRespV2(resp2))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp2)
			return resp2, nil
		}).Times(1)

	// Third scan with scanner ID - returns results
	s, err = hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.ScannerID(scannerID), hrpc.NumberOfRows(3))
	if err != nil {
		t.Fatal(err)
	}
	resp3 := &pb.ScanResponse{
		Results: dup(resultsPB[1:2]),
	}
	expected = append(expected, pbRespToRespV2(resp3))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region1)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp3)
			return resp3, nil
		}).Times(1)

	scannerID++

	// Fourth scan - new region with empty result
	s, err = hrpc.NewScanRange(scan.Context(), table,
		[]byte("bar"), nil, hrpc.NumberOfRows(3))
	if err != nil {
		t.Fatal(err)
	}
	resp4 := &pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: proto.Bool(true),
		Results:             nil,
	}
	expected = append(expected, pbRespToRespV2(resp4))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region2)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp4)
			return resp4, nil
		}).Times(1)

	// Fifth scan - returns actual results
	s, err = hrpc.NewScanRange(scan.Context(), table,
		[]byte("bar"), nil, hrpc.ScannerID(scannerID), hrpc.NumberOfRows(3))
	if err != nil {
		t.Fatal(err)
	}
	resp5 := &pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   dup(resultsPB[2:3]),
	}
	expected = append(expected, pbRespToRespV2(resp5))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region2)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp5)
			return resp5, nil
		}).Times(1)

	scannerID++

	// Sixth scan - final region
	s, err = hrpc.NewScanRange(scan.Context(), table, []byte("foo"), nil,
		hrpc.NumberOfRows(3))
	if err != nil {
		t.Fatal(err)
	}
	resp6 := &pb.ScanResponse{
		ScannerId:   cp(scannerID),
		Results:     dup(resultsPB[3:4]),
		MoreResults: proto.Bool(false),
	}
	expected = append(expected, pbRespToRespV2(resp6))
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).DoAndReturn(
		func(rpc hrpc.Call) (msg proto.Message, err error) {
			rpc.SetRegion(region3)
			rpc.(*hrpc.Scan).Response = pbRespToRespV2(resp6)
			return resp6, nil
		}).Times(1)

	var rs []*hrpc.ScanResponseV2
	for {
		r, err := scanner.Scan(3)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}

	if !cmp.Equal(expected, rs) {
		t.Fatalf("exp: %v\ngot: %v", expected, rs)
	}
}
