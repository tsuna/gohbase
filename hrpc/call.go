// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

// Call represents an HBase RPC call.
type Call interface {
	Table() []byte

	Key() []byte

	SetRegion(region []byte)
	Name() string
	Serialize() ([]byte, error)
	// Returns a newly created (default-state) protobuf in which to store the
	// response of this call.
	NewResponse() proto.Message

	GetResultChan() chan RPCResult

	GetContext() context.Context
}

// RPCResult is struct that will contain both the resulting message from an RPC
// call, and any erorrs that may have occurred related to makeing the RPC call.
type RPCResult struct {
	Msg   proto.Message
	Error error
}

type base struct {
	table []byte

	key []byte

	region []byte

	resultch chan RPCResult

	ctx context.Context
}

func (b *base) GetContext() context.Context {
	return b.ctx
}

func (b *base) SetRegion(region []byte) {
	b.region = region
}

func (b *base) regionSpecifier() *pb.RegionSpecifier {
	regionType := pb.RegionSpecifier_REGION_NAME
	return &pb.RegionSpecifier{
		Type:  &regionType,
		Value: []byte(b.region),
	}
}

func (b *base) Table() []byte {
	return b.table
}

func (b *base) Key() []byte {
	return b.key
}

func (b *base) GetResultChan() chan RPCResult {
	if b.resultch == nil {
		// Buffered channels, so that if a writer thread sends a message (or
		// reports an error) after the deadline it doesn't block due to the
		// requesting thread having moved on.
		b.resultch = make(chan RPCResult, 1)
	}
	return b.resultch
}
