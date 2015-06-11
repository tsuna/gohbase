// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
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

	GetDeadline() *time.Time
	GetResultChans() (chan proto.Message, chan error)
	NewResultChans() (chan proto.Message, chan error)
}

type base struct {
	table []byte

	key []byte

	region []byte

	deadline *time.Time
	resultch chan proto.Message
	errch    chan error
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

func (b *base) SetDeadline(dl *time.Time) {
	b.deadline = dl
}

func (b *base) GetDeadline() *time.Time {
	return b.deadline
}

func (b *base) NewResultChans() (chan proto.Message, chan error) {
	// Buffered channels, so that if a writer thread sends a message (or reports
	// an error) after the deadline it doesn't block due to the requesting
	// thread having moved on.
	b.resultch = make(chan proto.Message, 1)
	b.errch = make(chan error, 1)
	return b.resultch, b.errch
}

func (b *base) GetResultChans() (chan proto.Message, chan error) {
	return b.resultch, b.errch
}
