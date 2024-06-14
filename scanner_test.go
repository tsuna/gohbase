// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/test"
	"github.com/tsuna/gohbase/test/mock"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func cp(i uint64) *uint64 {
	j := i
	return &j
}

type scanMatcher struct {
	scan *hrpc.Scan
}

func (c *scanMatcher) Matches(x interface{}) bool {
	s, ok := x.(*hrpc.Scan)
	if c.scan.Region() == nil {
		c.scan.SetRegion(region.NewInfo(0, nil, nil, nil, nil, nil))
	}
	if s.Region() == nil {
		s.SetRegion(region.NewInfo(0, nil, nil, nil, nil, nil))
	}
	return ok && proto.Equal(c.scan.ToProto(), s.ToProto())
}

func (c *scanMatcher) String() string {
	return fmt.Sprintf("is equal to %s", c.scan)
}

var resultsPB = []*pb.Result{
	// region 1
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("a"), Family: []byte("A"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("a"), Family: []byte("A"), Qualifier: []byte("2")},
			&pb.Cell{Row: []byte("a"), Family: []byte("B"), Qualifier: []byte("1")},
		},
	},
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("b"), Family: []byte("A"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("b"), Family: []byte("B"), Qualifier: []byte("2")},
		},
	},
	// region 2
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("bar"), Family: []byte("C"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("baz"), Family: []byte("C"), Qualifier: []byte("2")},
			&pb.Cell{Row: []byte("baz"), Family: []byte("C"), Qualifier: []byte("2")},
		},
	},
	// region 3
	&pb.Result{
		Cell: []*pb.Cell{
			&pb.Cell{Row: []byte("yolo"), Family: []byte("D"), Qualifier: []byte("1")},
			&pb.Cell{Row: []byte("yolo"), Family: []byte("D"), Qualifier: []byte("2")},
		},
	},
}

var (
	table   = []byte("test")
	region1 = region.NewInfo(0, nil, table, []byte("table,,bar,whatever"), nil, []byte("bar"))
	region2 = region.NewInfo(0, nil, table,
		[]byte("table,bar,foo,whatever"), []byte("bar"), []byte("foo"))
	region3 = region.NewInfo(0, nil, table, []byte("table,foo,,whatever"), []byte("foo"), nil)
)

func dup(a []*pb.Result) []*pb.Result {
	b := make([]*pb.Result, len(a))
	copy(b, a)
	return b
}

func testCallClose(scan *hrpc.Scan, c *mock.MockRPCClient, scannerID uint64,
	group *sync.WaitGroup, t *testing.T) {
	//	t.Helper()

	s, err := hrpc.NewScanRange(context.Background(), table, nil, nil,
		hrpc.ScannerID(scannerID), hrpc.CloseScanner(), hrpc.NumberOfRows(0))
	if err != nil {
		t.Fatal(err)
	}

	c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(arg0 interface{}) {
		group.Done()
	}).Return(&pb.ScanResponse{}, nil).Times(1)
}
func TestScanner(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	var wg sync.WaitGroup
	wg.Add(3)
	defer wg.Wait()

	scan, err := hrpc.NewScan(context.Background(), table, hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}

	var scannerID uint64 = 42
	scanner := newScanner(c, scan)

	s, err := hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(&pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: proto.Bool(true),
		Results:             dup(resultsPB[:1]),
	}, nil).Times(1)

	s, err = hrpc.NewScanRange(scan.Context(), table, nil, nil,
		hrpc.ScannerID(scannerID), hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}

	c.EXPECT().SendRPC(&scanMatcher{
		scan: s,
	}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(&pb.ScanResponse{
		Results: dup(resultsPB[1:2]),
	}, nil).Times(1)

	// added call to close scanner
	testCallClose(scan, c, scannerID, &wg, t)
	scannerID++

	s, err = hrpc.NewScanRange(scan.Context(), table,
		[]byte("bar"), nil, hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region2)
	}).Return(&pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   dup(resultsPB[2:3]),
	}, nil).Times(1)

	// added call to close scanner
	testCallClose(scan, c, scannerID, &wg, t)
	if err != nil {
		t.Fatal(err)
	}
	scannerID++

	s, err = hrpc.NewScanRange(scan.Context(), table, []byte("foo"), nil,
		hrpc.NumberOfRows(2))
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region3)
	}).Return(&pb.ScanResponse{
		ScannerId:   cp(scannerID),
		Results:     dup(resultsPB[3:4]),
		MoreResults: proto.Bool(false),
	}, nil).Times(1)

	// added call to close scanner
	testCallClose(scan, c, scannerID, &wg, t)

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

	if !reflect.DeepEqual(expected, rs) {
		t.Fatalf("expected %v, got %v", expected, rs)
	}
}

var cells = []*pb.Cell{
	&pb.Cell{Row: []byte("a"), Family: []byte("A"), Qualifier: []byte("1")}, // 0
	&pb.Cell{Row: []byte("a"), Family: []byte("A"), Qualifier: []byte("2")},
	&pb.Cell{Row: []byte("a"), Family: []byte("A"), Qualifier: []byte("3")},
	&pb.Cell{Row: []byte("b"), Family: []byte("B"), Qualifier: []byte("1")},   // 3
	&pb.Cell{Row: []byte("b"), Family: []byte("B"), Qualifier: []byte("2")},   // 4
	&pb.Cell{Row: []byte("bar"), Family: []byte("B"), Qualifier: []byte("1")}, // 5
	&pb.Cell{Row: []byte("bar"), Family: []byte("B"), Qualifier: []byte("2")},
	&pb.Cell{Row: []byte("bar"), Family: []byte("B"), Qualifier: []byte("3")}, // 7
	&pb.Cell{Row: []byte("foo"), Family: []byte("F"), Qualifier: []byte("1")}, // 8
	&pb.Cell{Row: []byte("foo"), Family: []byte("F"), Qualifier: []byte("2")}, // 9
}

func TestPartialResults(t *testing.T) {
	scan, err := hrpc.NewScan(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	expected := []*hrpc.Result{
		hrpc.ToLocalResult(&pb.Result{Cell: cells[:3]}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[3:5]}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[5:8]}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[8:]}),
	}
	testPartialResults(t, scan, expected)
}

func TestAllowPartialResults(t *testing.T) {
	scan, err := hrpc.NewScan(context.Background(), table, hrpc.AllowPartialResults())
	if err != nil {
		t.Fatal(err)
	}
	expected := []*hrpc.Result{
		hrpc.ToLocalResult(&pb.Result{Cell: cells[:3], Partial: proto.Bool(true)}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[3:4], Partial: proto.Bool(true)}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[4:5], Partial: proto.Bool(true)}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[5:7], Partial: proto.Bool(true)}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[7:8], Partial: proto.Bool(true)}),
		// empty list
		hrpc.ToLocalResult(&pb.Result{Cell: cells[8:8], Partial: proto.Bool(true)}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[8:9], Partial: proto.Bool(true)}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[9:], Partial: proto.Bool(true)}),
	}
	testPartialResults(t, scan, expected)
}

func TestScanMetrics(t *testing.T) {
	scanned, filtered := rowsScanned, rowsFiltered
	i0, i1, i2, i4 := int64(0), int64(1), int64(2), int64(4)
	tcases := []struct {
		description          string
		trackScanMetrics     func(call hrpc.Call) error
		filter               func(call hrpc.Call) error
		results              []*pb.Result
		scanMetrics          *pb.ScanMetrics
		expectedResults      []*hrpc.Result
		expectedRowsScanned  int64
		expectedRowsFiltered int64
	}{
		{
			description: "ScanMetrics not enabled",
			results: []*pb.Result{
				{Cell: cells[:3]},
			},
			scanMetrics:     nil,
			expectedResults: []*hrpc.Result{hrpc.ToLocalResult(&pb.Result{Cell: cells[:3]})},
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
			expectedResults:      []*hrpc.Result{toLocalResult(&pb.Result{Cell: cells[:3]})},
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
			expectedResults:      []*hrpc.Result{toLocalResult(&pb.Result{Cell: cells[:5]})},
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
			expectedResults:      []*hrpc.Result{toLocalResult(&pb.Result{Cell: cells})},
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
			expectedResults:      []*hrpc.Result{toLocalResult(&pb.Result{Cell: cells[:5]})},
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
			expectedResults:      []*hrpc.Result{toLocalResult(&pb.Result{Cell: cells[:3]})},
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

			sc := newScanner(c, scan)

			c.EXPECT().SendRPC(&scanMatcher{scan: scan}).Return(&pb.ScanResponse{
				Results:     tcase.results,
				ScanMetrics: tcase.scanMetrics,
			}, nil).Times(1)

			var res []*hrpc.Result
			for {
				var r *hrpc.Result
				r, err = sc.Next()
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

			if !reflect.DeepEqual(tcase.expectedResults, res) {
				t.Fatalf("expected: %+v\ngot: %+v", tcase.expectedResults, res)
			}
		})
	}
}

func TestErrorScanFromID(t *testing.T) {
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

func TestErrorScanFromIDAllowPartials(t *testing.T) {
	scan, err := hrpc.NewScan(context.Background(), table, hrpc.AllowPartialResults())
	if err != nil {
		t.Fatal(err)
	}
	expected := []*hrpc.Result{
		hrpc.ToLocalResult(&pb.Result{Cell: cells[:3]}),
		hrpc.ToLocalResult(&pb.Result{Cell: cells[3:4]}),
	}
	testErrorScanFromID(t, scan, expected)
}

func TestErrorFirstFetch(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	scan, err := hrpc.NewScan(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	scanner := newScanner(c, scan)

	srange, err := hrpc.NewScanRange(context.Background(), table, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	outErr := errors.New("WTF")
	c.EXPECT().SendRPC(&scanMatcher{scan: srange}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(nil, outErr).Times(1)

	var r *hrpc.Result
	var rs []*hrpc.Result
	for {
		r, err = scanner.Next()
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
	if len(rs) != 0 {
		t.Fatalf("expected no results, got %v", rs)
	}
}

func testErrorScanFromID(t *testing.T, scan *hrpc.Scan, out []*hrpc.Result) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	var scannerID uint64 = 42
	scanner := newScanner(c, scan)

	srange, err := hrpc.NewScanRange(scan.Context(), table, nil, nil, scan.Options()...)
	if err != nil {
		t.Fatal(err)
	}

	c.EXPECT().SendRPC(&scanMatcher{scan: srange}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(&pb.ScanResponse{
		ScannerId:           cp(scannerID),
		MoreResultsInRegion: proto.Bool(true),
		Results: []*pb.Result{
			&pb.Result{Cell: cells[:3]},
			&pb.Result{Cell: cells[3:4]},
		},
	}, nil).Times(1)

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

	var r *hrpc.Result
	var rs []*hrpc.Result
	for {
		r, err = scanner.Next()
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

func testPartialResults(t *testing.T, scan *hrpc.Scan, expected []*hrpc.Result) {
	t.Helper()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	var wg sync.WaitGroup
	wg.Add(3)
	defer wg.Wait()

	tcase := []struct {
		region              hrpc.RegionInfo
		results             []*pb.Result
		moreResultsInRegion bool
		scanFromID          bool
	}{
		{
			region: region1,
			results: []*pb.Result{
				&pb.Result{Cell: cells[:3], Partial: proto.Bool(true)},
				&pb.Result{Cell: cells[3:4], Partial: proto.Bool(true)},
			},
			moreResultsInRegion: true,
		},
		{ // end of region, should return row b
			region: region1,
			results: []*pb.Result{
				&pb.Result{Cell: cells[4:5], Partial: proto.Bool(true)},
			},
			scanFromID: true,
		},
		{ // half a row in a result in the same response - unlikely, but why not
			region: region2,
			results: []*pb.Result{
				&pb.Result{Cell: cells[5:7], Partial: proto.Bool(true)},
				&pb.Result{Cell: cells[7:8], Partial: proto.Bool(true)},
			},
			moreResultsInRegion: true,
		},
		{ // empty result, last in region
			region:     region2,
			results:    []*pb.Result{&pb.Result{Cell: cells[8:8], Partial: proto.Bool(true)}},
			scanFromID: true,
		},
		{
			region: region3,
			results: []*pb.Result{
				&pb.Result{Cell: cells[8:9], Partial: proto.Bool(true)},
			},
			moreResultsInRegion: true,
		},
		{ // last row
			region: region3,
			results: []*pb.Result{
				&pb.Result{Cell: cells[9:], Partial: proto.Bool(true)},
			},
			scanFromID: true,
		},
	}

	var scannerID uint64
	scanner := newScanner(c, scan)
	ctx := scan.Context()
	for _, partial := range tcase {
		partial := partial
		var s *hrpc.Scan
		var err error
		if partial.scanFromID {
			s, err = hrpc.NewScanRange(ctx, table, partial.region.StartKey(), nil,
				hrpc.ScannerID(scannerID))
		} else {
			s, err = hrpc.NewScanRange(ctx, table, partial.region.StartKey(), nil,
				scan.Options()...)
			scannerID++
		}
		if err != nil {
			t.Fatal(err)
		}

		c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(rpc hrpc.Call) {
			rpc.SetRegion(partial.region)
		}).Return(&pb.ScanResponse{
			ScannerId:           cp(scannerID),
			MoreResultsInRegion: &partial.moreResultsInRegion,
			Results:             partial.results,
		}, nil).Times(1)

		if partial.scanFromID {
			// added call to close scanner
			testCallClose(scan, c, scannerID, &wg, t)
		}
	}

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

	if !reflect.DeepEqual(expected, rs) {
		t.Fatalf("expected %v, got %s", expected, rs)
	}
}

func TestReversedScanner(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	var wg sync.WaitGroup
	wg.Add(3)
	defer wg.Wait()

	ctx := context.Background()
	scan, err := hrpc.NewScan(ctx, table, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}

	var scannerID uint64 = 42

	scanner := newScanner(c, scan)
	ctx = scan.Context()
	s, err := hrpc.NewScanRange(ctx, table, nil, nil, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region3)
	}).Return(&pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   dup(resultsPB[3:4]),
	}, nil).Times(1)

	// added call to close scanner
	testCallClose(scan, c, scannerID, &wg, t)

	s, err = hrpc.NewScanRange(ctx, table,
		append([]byte("fon"), rowPadding...), nil, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region2)
	}).Return(&pb.ScanResponse{
		ScannerId: cp(scannerID),
		Results:   dup(resultsPB[2:3]),
	}, nil).Times(1)

	// added call to close scanner
	testCallClose(scan, c, scannerID, &wg, t)

	s, err = hrpc.NewScanRange(ctx, table,
		append([]byte("baq"), rowPadding...), nil, hrpc.Reversed())
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{scan: s}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(&pb.ScanResponse{
		MoreResultsInRegion: proto.Bool(true),
		ScannerId:           cp(scannerID),
		Results:             dup(resultsPB[1:2]),
	}, nil).Times(1)

	s, err = hrpc.NewScanRange(ctx, table, nil, nil, hrpc.ScannerID(scannerID))
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().SendRPC(&scanMatcher{
		scan: s,
	}).Do(func(rpc hrpc.Call) {
		rpc.SetRegion(region1)
	}).Return(&pb.ScanResponse{
		Results: dup(resultsPB[:1]),
	}, nil).Times(1)

	// added call to close scanner
	testCallClose(scan, c, scannerID, &wg, t)

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
	for i := len(resultsPB) - 1; i >= 0; i-- {
		expected = append(expected, hrpc.ToLocalResult(resultsPB[i]))
	}

	if !reflect.DeepEqual(expected, rs) {
		t.Fatalf("expected %v, got %v", expected, rs)
	}
}

func TestScannerWithContextCanceled(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	scan, err := hrpc.NewScan(ctx, []byte(t.Name()))
	if err != nil {
		t.Fatal(err)
	}

	scanner := newScanner(c, scan)

	cancel()

	_, err = scanner.Next()
	if err != context.Canceled {
		t.Fatalf("unexpected error %v, expected %v", err, context.Canceled)
	}
}

func TestScannerClosed(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	c := mock.NewMockRPCClient(ctrl)

	scan, err := hrpc.NewScan(context.Background(), []byte(t.Name()))
	if err != nil {
		t.Fatal(err)
	}

	scanner := newScanner(c, scan)
	scanner.Close()

	_, err = scanner.Next()
	if err != io.EOF {
		t.Fatalf("unexpected error %v, expected %v", err, io.EOF)
	}
}
