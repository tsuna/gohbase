// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"google.golang.org/protobuf/proto"
)

func makePBCell(row, family, qual string, value []byte) *pb.Cell {
	cell := &pb.Cell{}
	cell.SetRow([]byte(row))
	cell.SetFamily([]byte(family))
	cell.SetQualifier([]byte(qual))
	cell.SetValue(value)
	return cell
}

type testClient struct {
	addr    string
	numNSRE int32
}

var nsreRegion = &pb.Result{Cell: []*pb.Cell{
	makePBCell("nsre,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "regioninfo",
		[]byte("PBUF\b\xc4\xcd\xe9\x99\xe0)\x12\x0f\n\adefault\x12\x04nsre"+
			"\x1a\x00\"\x00(\x000\x008\x00")),
	makePBCell("nsre,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "seqnumDuringOpen",
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
	makePBCell("nsre,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "server",
		[]byte("regionserver:1")),
	makePBCell("nsre,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "serverstartcode",
		[]byte("\x00\x00\x01N\x02\x92R\xb1")),
}}

// makeRegionResult returns a region that spans the whole table
// and uses name of the table as the hostname of the regionserver
func makeRegionResult(key []byte) *pb.ScanResponse {
	s := bytes.SplitN(key, []byte(","), 2)
	fqtable := s[0]

	row := append(fqtable, []byte(",,1434573235908.56f833d5569a27c7a43fbf547b4924a4.")...)
	t := bytes.SplitN(fqtable, []byte{':'}, 2)
	var namespace, table []byte
	if len(t) == 2 {
		namespace = t[0]
		table = t[1]
	} else {
		namespace = []byte("default")
		table = fqtable
	}
	regionInfo := &pb.RegionInfo{
		RegionId: proto.Uint64(1434573235908),
		TableName: &pb.TableName{
			Namespace: namespace,
			Qualifier: table,
		},
		Offline: proto.Bool(false),
	}
	regionInfoValue, err := proto.Marshal(regionInfo)
	if err != nil {
		panic(err)
	}
	regionInfoValue = append([]byte("PBUF"), regionInfoValue...)

	cell1 := &pb.Cell{}
	cell1.SetRow(row)
	cell1.SetFamily([]byte("info"))
	cell1.SetQualifier([]byte("regioninfo"))
	cell1.SetValue(regionInfoValue)

	cell2 := &pb.Cell{}
	cell2.SetRow(row)
	cell2.SetFamily([]byte("info"))
	cell2.SetQualifier([]byte("seqnumDuringOpen"))
	cell2.SetValue([]byte("\x00\x00\x00\x00\x00\x00\x00\x02"))

	cell3 := &pb.Cell{}
	cell3.SetRow(row)
	cell3.SetFamily([]byte("info"))
	cell3.SetQualifier([]byte("server"))
	cell3.SetValue(fqtable)

	cell4 := &pb.Cell{}
	cell4.SetRow(row)
	cell4.SetFamily([]byte("info"))
	cell4.SetQualifier([]byte("serverstartcode"))
	cell4.SetValue([]byte("\x00\x00\x01N\x02\x92R\xb1"))

	return &pb.ScanResponse{Results: []*pb.Result{
		&pb.Result{Cell: []*pb.Cell{cell1, cell2, cell3, cell4}}}}
}

var metaRow = &pb.Result{Cell: []*pb.Cell{
	makePBCell("test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "regioninfo",
		[]byte("PBUF\b\xc4\xcd\xe9\x99\xe0)\x12\x0f\n\adefault\x12\x04test"+
			"\x1a\x00\"\x00(\x000\x008\x00")),
	makePBCell("test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "seqnumDuringOpen",
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
	makePBCell("test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "server",
		[]byte("regionserver:2")),
	makePBCell("test,,1434573235908.56f833d5569a27c7a43fbf547b4924a4.", "info", "serverstartcode",
		[]byte("\x00\x00\x01N\x02\x92R\xb1")),
}}

var test1SplitA = &pb.Result{Cell: []*pb.Cell{
	makePBCell("test1,,1480547738107.825c5c7e480c76b73d6d2bad5d3f7bb8.", "info", "regioninfo",
		[]byte("PBUF\b\xfbÖ\xbc\x8b+\x12\x10\n\adefault\x12\x05"+
			"test1\x1a\x00\"\x03baz(\x000\x008\x00")),
	makePBCell("test1,,1480547738107.825c5c7e480c76b73d6d2bad5d3f7bb8.", "info", "seqnumDuringOpen",
		[]byte("\x00\x00\x00\x00\x00\x00\x00\v")),
	makePBCell("test1,,1480547738107.825c5c7e480c76b73d6d2bad5d3f7bb8.", "info", "server",
		[]byte("regionserver:1")),
	makePBCell("test1,,1480547738107.825c5c7e480c76b73d6d2bad5d3f7bb8.", "info", "serverstartcode",
		[]byte("\x00\x00\x01X\xb6\x83^3")),
}}

var m sync.RWMutex
var clients map[string]uint32

func init() {
	clients = make(map[string]uint32)
}

func newMockRegionClient(addr string, ctype region.ClientType,
	options *region.RegionClientOptions) hrpc.RegionClient {
	m.Lock()
	clients[addr]++
	m.Unlock()
	return &testClient{addr: addr}
}

func (c *testClient) Dial(ctx context.Context) error {
	return nil
}

func (c *testClient) Addr() string {
	return c.addr
}

func (c *testClient) String() string {
	return fmt.Sprintf("RegionClient{Addr: %s}", c.addr)
}

func (c *testClient) QueueRPC(call hrpc.Call) {
	// ignore timed out rpcs to mock the region client
	select {
	case <-call.Context().Done():
		return
	default:
	}
	if !bytes.Equal(call.Table(), []byte("hbase:meta")) {
		_, ok := call.(*hrpc.Get)
		if !ok || !bytes.HasSuffix(call.Key(), bytes.Repeat([]byte{0}, 17)) {
			// not a get and not a region probe
			// just return as the mock call should just populate the ResultChan in test
			return
		}
		// region probe, fail for the nsre region 3 times to force retry
		if bytes.Equal(call.Table(), []byte("nsre")) {
			i := atomic.AddInt32(&c.numNSRE, 1)
			if i <= 3 {
				call.ResultChan() <- hrpc.RPCResult{Error: region.NotServingRegionError{}}
				return
			}
		}
		m.RLock()
		i := clients[c.addr]
		m.RUnlock()

		// if we are connected to this client the first time,
		// pretend it's down to fail the probe and start a reconnect
		if bytes.Equal(call.Table(), []byte("down")) {
			if i <= 1 {
				call.ResultChan() <- hrpc.RPCResult{Error: region.ServerError{}}
			} else {
				// otherwise, the region is fine
				call.ResultChan() <- hrpc.RPCResult{}
			}
			return
		}
	}

	returnScanResponse := func(call hrpc.Call, resp *pb.ScanResponse) {
		call.(*hrpc.Scan).Response = pbRespToRespV2(resp)
		call.ResultChan() <- hrpc.RPCResult{Msg: resp}
	}

	if bytes.HasSuffix(call.Key(), bytes.Repeat([]byte{0}, 17)) {
		// meta region probe, return empty to signify that region is online
		call.ResultChan() <- hrpc.RPCResult{}
	} else if bytes.HasPrefix(call.Key(), []byte("test,")) {
		returnScanResponse(call, &pb.ScanResponse{Results: []*pb.Result{metaRow}})
	} else if bytes.HasPrefix(call.Key(), []byte("test1,,")) {
		returnScanResponse(call, &pb.ScanResponse{Results: []*pb.Result{test1SplitA}})
	} else if bytes.HasPrefix(call.Key(), []byte("nsre,,")) {
		returnScanResponse(call, &pb.ScanResponse{Results: []*pb.Result{nsreRegion}})
	} else if bytes.HasPrefix(call.Key(), []byte("tablenotfound,")) {
		returnScanResponse(call, &pb.ScanResponse{
			Results:     []*pb.Result{},
			MoreResults: proto.Bool(false),
		})
	} else {
		returnScanResponse(call, makeRegionResult(call.Key()))
	}
}

func (c *testClient) QueueBatch(ctx context.Context, batch []hrpc.Call) {
	// do nothing. Let the test fill in result.
}

func (c *testClient) Close() {}

func pbRespToRespV2(resp *pb.ScanResponse) *hrpc.ScanResponseV2 {
	respv2 := &hrpc.ScanResponseV2{
		Results: make([]hrpc.ResultV2, len(resp.Results)),
	}
	for i, res := range resp.Results {
		cellsV2 := make([]hrpc.CellV2, len(res.Cell))
		for i, cell := range res.Cell {
			var err error
			cellsV2[i], err = hrpc.NewCellV2(
				cell.GetRow(),
				cell.GetFamily(),
				cell.GetQualifier(),
				cell.GetTimestamp(),
				cell.GetValue(),
				cell.GetCellType(),
			)
			if err != nil {
				panic(err)
			}
		}
		respv2.Results[i] = hrpc.ResultV2{
			Cells:   cellsV2,
			Partial: res.GetPartial(),
		}
	}
	return respv2
}
