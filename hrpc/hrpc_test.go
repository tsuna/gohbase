// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/aristanetworks/goarista/test"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
)

func TestNewGet(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	key := "45"
	keyb := []byte(key)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	get, err := NewGet(ctx, tableb, keyb)
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, nil, nil) {
		t.Errorf("Get1 didn't set attributes correctly.")
	}
	get, err = NewGetStr(ctx, table, key)
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, nil, nil) {
		t.Errorf("Get2 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Families(fam))
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, fam, nil) {
		t.Errorf("Get3 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1))
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, nil, filter1) {
		t.Errorf("Get4 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1), Families(fam))
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, fam, filter1) {
		t.Errorf("Get5 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1))
	err = Families(fam)(get)
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, fam, filter1) {
		t.Errorf("Get6 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, MaxVersions(math.MaxInt32))
	if err != nil {
		t.Errorf("Get7 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, MaxVersions(math.MaxInt32+1))
	errStr := "'MaxVersions' exceeds supported number of versions"
	if err != nil && errStr != err.Error() || err == nil {
		t.Errorf("Get8 Expected: %#v\nReceived: %#v", errStr, err)
	}
}

func confirmGetAttributes(ctx context.Context, g *Get, table, key []byte,
	fam map[string][]string, filter1 filter.Filter) bool {
	if g.Context() != ctx ||
		!bytes.Equal(g.Table(), table) ||
		!bytes.Equal(g.Key(), key) ||
		!reflect.DeepEqual(g.families, fam) ||
		(filter1 != nil && g.filter == nil) {
		return false
	}
	return true
}

func TestNewScan(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	start := "0"
	stop := "100"
	startb := []byte("0")
	stopb := []byte("100")
	scan, err := NewScan(ctx, tableb)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan1 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, startb, stopb, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan2 didn't set attributes correctly.")
	}
	scan, err = NewScanStr(ctx, table)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan3 didn't set attributes correctly.")
	}
	scan, err = NewScanRangeStr(ctx, table, start, stop)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, startb, stopb, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan4 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb, Families(fam), Filters(filter1))
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, startb, stopb, fam, filter1,
		DefaultNumberOfRows) {
		t.Errorf("Scan5 didn't set attributes correctly.")
	}
	scan, err = NewScan(ctx, tableb, Filters(filter1), Families(fam))
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, fam, filter1,
		DefaultNumberOfRows) {
		t.Errorf("Scan6 didn't set attributes correctly.")
	}
	scan, err = NewScan(ctx, tableb, NumberOfRows(1))
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, nil, nil, 1) {
		t.Errorf("Scan7 didn't set number of versions correctly")
	}
}

type mockRegionInfo []byte

func (ri mockRegionInfo) Name() []byte {
	return []byte(ri)
}

func (ri mockRegionInfo) IsUnavailable() bool               { return true }
func (ri mockRegionInfo) AvailabilityChan() <-chan struct{} { return nil }
func (ri mockRegionInfo) MarkUnavailable() bool             { return true }
func (ri mockRegionInfo) MarkAvailable()                    {}
func (ri mockRegionInfo) MarkDead()                         {}
func (ri mockRegionInfo) Context() context.Context          { return nil }
func (ri mockRegionInfo) String() string                    { return "" }
func (ri mockRegionInfo) ID() uint64                        { return 0 }
func (ri mockRegionInfo) StartKey() []byte                  { return nil }
func (ri mockRegionInfo) StopKey() []byte                   { return nil }
func (ri mockRegionInfo) Namespace() []byte                 { return nil }
func (ri mockRegionInfo) Table() []byte                     { return nil }
func (ri mockRegionInfo) SetClient(RegionClient)            {}
func (ri mockRegionInfo) Client() RegionClient              { return nil }

type byFamily []*pb.MutationProto_ColumnValue

func (f byFamily) Len() int      { return len(f) }
func (f byFamily) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f byFamily) Less(i, j int) bool {
	return bytes.Compare(f[i].Family, f[j].Family) < 0
}

type byQualifier []*pb.MutationProto_ColumnValue_QualifierValue

func (q byQualifier) Len() int      { return len(q) }
func (q byQualifier) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q byQualifier) Less(i, j int) bool {
	return bytes.Compare(q[i].Qualifier, q[j].Qualifier) < 0
}

func TestMutate(t *testing.T) {
	var (
		ctx   = context.Background()
		table = "table"
		key   = "key"
		rs    = &pb.RegionSpecifier{
			Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte("region"),
		}
	)

	tests := []struct {
		in  func() (*Mutate, error)
		out *pb.MutateRequest
		err error
	}{
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, nil, Durability(SkipWal))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_SKIP_WAL.Enum(),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, nil, Durability(DurabilityType(42)))
			},
			err: errors.New("invalid durability value"),
		},
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, nil, TTL(time.Second))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Attribute: []*pb.NameBytesPair{
						&pb.NameBytesPair{
							Name:  &attributeNameTTL,
							Value: []byte("\x00\x00\x00\x00\x00\x00\x03\xe8"),
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				})
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("value"),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, map[string]map[string][]byte{
					"cf1": map[string][]byte{
						"q1": []byte("value"),
						"q2": []byte("value"),
					},
					"cf2": map[string][]byte{
						"q1": []byte("value"),
					},
				})
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf1"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q1"),
									Value:     []byte("value"),
								},
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q2"),
									Value:     []byte("value"),
								},
							},
						},
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf2"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q1"),
									Value:     []byte("value"),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, Timestamp(time.Unix(0, 42*1e6)))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:  proto.Uint64(42),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("value"),
									Timestamp: proto.Uint64(42),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPutStr(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, TimestampUint64(42))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:  proto.Uint64(42),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("value"),
									Timestamp: proto.Uint64(42),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDelStr(ctx, table, key, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDelStr(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, TimestampUint64(42))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:  proto.Uint64(42),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte("q"),
									Value:      []byte("value"),
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum(),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewAppStr(ctx, table, key, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_APPEND.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewIncStr(ctx, table, key, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_INCREMENT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewIncStrSingle(ctx, table, key, "cf", "q", 1)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_INCREMENT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDelStr(ctx, table, key, map[string]map[string][]byte{
					"cf": nil,
				})
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte{},
									DeleteType: pb.MutationProto_DELETE_FAMILY.Enum(),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDelStr(ctx, table, key, map[string]map[string][]byte{
					"cf": nil,
				}, TimestampUint64(42))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					Timestamp:  proto.Uint64(42),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte{},
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_FAMILY.Enum(),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDelStr(ctx, table, key, map[string]map[string][]byte{
					"cf": nil,
				}, TimestampUint64(42), DeleteOneVersion())
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					Timestamp:  proto.Uint64(42),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte{},
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_FAMILY_VERSION.Enum(),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDelStr(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"a": nil,
					},
				}, TimestampUint64(42), DeleteOneVersion())
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					Timestamp:  proto.Uint64(42),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte("a"),
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_ONE_VERSION.Enum(),
								},
							},
						},
					},
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDelStr(ctx, table, key, nil, DeleteOneVersion())
			},
			err: errors.New(
				"'DeleteOneVersion' option cannot be specified for delete entire row request"),
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m, err := tcase.in()
			if d := test.Diff(tcase.err, err); d != "" {
				t.Fatalf("unexpected error: %s", d)
			}
			if tcase.err != nil {
				return
			}

			if m.Name() != "Mutate" {
				t.Fatalf("Expected name to be 'Mutate', got %s", m.Name())
			}

			_, ok := m.NewResponse().(*pb.MutateResponse)
			if !ok {
				t.Fatalf("Expected response to have type 'pb.MutateResponse', got %T",
					m.NewResponse())
			}

			m.SetRegion(mockRegionInfo([]byte("region")))
			p := m.ToProto()
			mr := p.(*pb.MutateRequest)

			sort.Sort(byFamily(mr.Mutation.ColumnValue))
			for _, cv := range mr.Mutation.ColumnValue {
				sort.Sort(byQualifier(cv.QualifierValue))
			}

			if d := test.Diff(tcase.out, mr); d != "" {
				t.Fatalf("unexpected error: %s", d)
			}
		})
	}
}

var expectedCells = []*pb.Cell{
	&pb.Cell{
		Row:       []byte("row7"),
		Family:    []byte("cf"),
		Qualifier: []byte("b"),
		Timestamp: proto.Uint64(1494873081120),
		Value:     []byte("Hello my name is Dog."),
	},
	&pb.Cell{
		Row:       []byte("row7"),
		Family:    []byte("cf"),
		Qualifier: []byte("a"),
		Timestamp: proto.Uint64(1494873081120),
		Value:     []byte("Hello my name is Dog."),
		CellType:  pb.CellType_PUT.Enum(),
	},
}
var cellblock = []byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
	102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
	97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46}

func TestDeserializeCellBlocksGet(t *testing.T) {
	// the first cell is already in protobuf
	getResp := &pb.GetResponse{Result: &pb.Result{
		Cell:                []*pb.Cell{expectedCells[0]},
		AssociatedCellCount: proto.Int32(1),
	}}
	g := &Get{}
	n, err := g.DeserializeCellBlocks(getResp, cellblock)
	if err != nil {
		t.Error(err)
	} else if d := test.Diff(expectedCells, getResp.Result.Cell); len(d) != 0 {
		t.Error(d)
	}
	if int(n) != len(cellblock) {
		t.Errorf("expected read %d, got read %d", len(cellblock), n)
	}

	// test error case
	getResp = &pb.GetResponse{Result: &pb.Result{
		AssociatedCellCount: proto.Int32(1),
	}}
	_, err = g.DeserializeCellBlocks(getResp, cellblock[:10])
	if err == nil {
		t.Error("expected error, got none")
	}
}

func TestDeserializeCellblocksMutate(t *testing.T) {
	// the first cell is already in protobuf
	mResp := &pb.MutateResponse{Result: &pb.Result{
		Cell:                []*pb.Cell{expectedCells[0]},
		AssociatedCellCount: proto.Int32(1),
	}}
	m := &Mutate{}
	n, err := m.DeserializeCellBlocks(mResp, cellblock)
	if err != nil {
		t.Error(err)
	}
	if d := test.Diff(expectedCells, mResp.Result.Cell); len(d) != 0 {
		t.Error(d)
	}
	if int(n) != len(cellblock) {
		t.Errorf("expected read %d, got read %d", len(cellblock), n)
	}

	// test error case
	mResp = &pb.MutateResponse{Result: &pb.Result{
		Cell:                expectedCells[:1],
		AssociatedCellCount: proto.Int32(1),
	}}
	_, err = m.DeserializeCellBlocks(mResp, cellblock[:10])
	if err == nil {
		t.Error("expected error, got none")
	}
}

func TestDeserializeCellBlocksScan(t *testing.T) {
	expectedResults := []*pb.Result{
		&pb.Result{
			Cell: []*pb.Cell{
				&pb.Cell{
					Row:       []byte("row7"),
					Family:    []byte("cf"),
					Qualifier: []byte("c"),
					Timestamp: proto.Uint64(1494873081120),
					Value:     []byte("Hello my name is Dog."),
					CellType:  pb.CellType_PUT.Enum(),
				},
				&pb.Cell{
					Row:       []byte("row7"),
					Family:    []byte("cf"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(1494873081120),
					Value:     []byte("Hello my name is Dog."),
					CellType:  pb.CellType_PUT.Enum(),
				},
			},
			Partial: proto.Bool(true),
		},
		&pb.Result{
			Cell: []*pb.Cell{
				&pb.Cell{
					Row:       []byte("row7"),
					Family:    []byte("cf"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(1494873081120),
					Value:     []byte("Hello my name is Dog."),
					CellType:  pb.CellType_PUT.Enum(),
				},
			},
			Partial: proto.Bool(false),
		},
	}
	cellblocks := []byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 99, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46,
		0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 98, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46,
		0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46}

	scanResp := &pb.ScanResponse{
		Results:              []*pb.Result{},
		PartialFlagPerResult: []bool{true, false},
		CellsPerResult:       []uint32{2, 1},
	}
	s := &Scan{}
	n, err := s.DeserializeCellBlocks(scanResp, cellblocks)
	if err != nil {
		t.Error(err)
	} else if d := test.Diff(expectedResults, scanResp.Results); len(d) != 0 {
		t.Error(d)
	}
	if int(n) != len(cellblocks) {
		t.Errorf("expected read %d, got read %d", len(cellblock), n)
	}

	// test error case
	scanResp = &pb.ScanResponse{
		PartialFlagPerResult: []bool{true, false},
		CellsPerResult:       []uint32{2, 1},
	}
	_, err = s.DeserializeCellBlocks(scanResp, cellblocks[:10])
	if err == nil {
		t.Error("expected error, got none")
	}
}

func confirmScanAttributes(ctx context.Context, s *Scan, table, start, stop []byte,
	fam map[string][]string, fltr filter.Filter, numberOfRows uint32) bool {
	if fltr == nil && s.filter != nil {
		return false
	}
	return s.Context() == ctx &&
		bytes.Equal(s.Table(), table) &&
		bytes.Equal(s.StartRow(), start) &&
		bytes.Equal(s.StopRow(), stop) &&
		reflect.DeepEqual(s.families, fam) &&
		s.numberOfRows == numberOfRows
}

func BenchmarkMutateToProtoWithNestedMaps(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := map[string]map[string][]byte{
			"cf": map[string][]byte{
				"a": []byte{10},
				"b": []byte{20},
				"c": []byte{30, 0},
				"d": []byte{40, 0, 0, 0},
				"e": []byte{50, 0, 0, 0, 0, 0, 0, 0},
				"f": []byte{60},
				"g": []byte{70},
				"h": []byte{80, 0},
				"i": []byte{90, 0, 0, 0},
				"j": []byte{100, 0, 0, 0, 0, 0, 0, 0},
				"k": []byte{0, 0, 220, 66},
				"l": []byte{0, 0, 0, 0, 0, 0, 94, 64},
				"m": []byte{0, 0, 2, 67, 0, 0, 0, 0},
				"n": []byte{0, 0, 0, 0, 0, 128, 97, 64, 0, 0, 0, 0, 0, 0, 0, 0},
				"o": []byte{150},
				"p": []byte{4, 8, 15, 26, 23, 42},
				"q": []byte{1, 1, 3, 5, 8, 13, 21, 34, 55},
				"r": []byte("This is a test string."),
			},
		}
		mutate, err := NewPutStr(context.Background(), "", "", data)
		if err != nil {
			b.Errorf("Error creating mutate: %v", err)
		}

		if p := mutate.ToProto(); p == nil {
			b.Fatal("got a nil proto")
		}
	}
}
