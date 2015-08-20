// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/regioninfo"
	"golang.org/x/net/context"
)

var index uint32

// MultiRequest represents a request composed of many request
type multiRequest struct {
	RegionActions map[*regioninfo.Info][]hrpc.Call
}

func newMultiRequest(ras map[*regioninfo.Info][]hrpc.Call) *multiRequest {
	mr := &multiRequest{
		RegionActions: ras,
	}
	return mr
}

// Table only exists to implement the hrpc.Call interface
func (mr multiRequest) Table() []byte {
	return []byte{}
}

// Key only exists to implement the hrpc.Call interface
func (mr multiRequest) Key() []byte {
	return []byte{}
}

// GetRegion only exists to implement the hrpc.Call interface
func (mr multiRequest) GetRegion() *regioninfo.Info {
	return &regioninfo.Info{}
}

// SetRegion only exists to implement the hrpc.Call interface
func (mr multiRequest) SetRegion(region *regioninfo.Info) {
	return
}

// GetName returns the name of this RPC call.
func (mr multiRequest) GetName() string {
	return "Multi"
}

// Serialize serializes this RPC into a buffer.
func (mr multiRequest) Serialize() (proto.Message, error) {
	var rActions []*pb.RegionAction
	for _, rpcs := range mr.RegionActions {
		regSpec := rpcs[0].RegionSpecifier()
		actions := make([]*pb.Action, len(rpcs))
		for i, rpc := range rpcs {
			val, err := rpc.Serialize()
			if err != nil {
				return nil, err
			}
			tmpindex := index
			switch v := val.(type) {
			case *pb.GetRequest:
				actions[i] = &pb.Action{
					Index: &tmpindex,
					Get:   v.Get,
				}
			case *pb.MutateRequest:
				actions[i] = &pb.Action{
					Index:    &tmpindex,
					Mutation: v.Mutation,
				}
			}
			index++
		}
		rActions = append(rActions, &pb.RegionAction{
			Region: regSpec,
			Action: actions,
		})
	}
	pbmr := &pb.MultiRequest{
		RegionAction: rActions,
	}
	return pbmr, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (mr multiRequest) NewResponse() proto.Message {
	return &pb.MultiResponse{}
}

// GetResultChan only exists to implement the hrpc.Call interface
func (mr multiRequest) GetResultChan() chan hrpc.RPCResult {
	return nil
}

// GetContext only exists to implement the hrpc.Call interface
func (mr multiRequest) GetContext() context.Context {
	return context.Background()
}

// SetFamilies only exists to implement the hrpc.Call interface
func (mr multiRequest) SetFamilies(fam map[string][]string) error {
	return nil
}

// SetFilter only exists to implement the hrpc.Call interface
func (mr multiRequest) SetFilter(ft filter.Filter) error {
	return nil
}

// RegionSpecifier only exists to implement the hrpc.Call interface
func (mr multiRequest) RegionSpecifier() *pb.RegionSpecifier {
	return nil
}

func nbpToErr(nbp *pb.NameBytesPair) error {
	if nbp == nil {
		return nil
	}
	return fmt.Errorf("%s: %s", nbp.Name, string(nbp.Value))
}

func (mr multiRequest) SendResults(msg proto.Message, err error) {
	if err != nil {
		for _, rpcs := range mr.RegionActions {
			for _, rpc := range rpcs {
				rpc.GetResultChan() <- hrpc.RPCResult{Msg: nil, Error: err}
			}
		}
		return
	}

	mresp := msg.(*pb.MultiResponse)
	regActCounter := 0

	for _, rpcs := range mr.RegionActions {
		regActRes := mresp.RegionActionResult[regActCounter]
		if regActRes.Exception != nil {
			for _, rpc := range rpcs {
				rpc.GetResultChan() <- hrpc.RPCResult{
					Msg:   nil,
					Error: nbpToErr(regActRes.Exception),
				}
			}
			continue
		}
		for i, rpc := range rpcs {
			resExp := regActRes.ResultOrException[i]
			rpc.GetResultChan() <- hrpc.RPCResult{
				Msg:   resExp.Result,
				Error: nbpToErr(resExp.Exception),
			}
		}
		regActCounter++
	}
}
