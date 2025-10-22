package hrpc

import (
	"context"
	"errors"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// ListReplicationPeers models a ListReplicationPeers pb call
type ListReplicationPeers struct {
	base
	regex string
}

// ListReplicationPeersRegex sets a regex for ListReplicationPeers
func ListReplicationPeersRegex(regex string) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListReplicationPeers)
		if !ok {
			return errors.New("ListReplicationPeersRegex option can only be used with " +
				"ListReplicationPeers")
		}
		l.regex = regex
		return nil
	}
}

// NewListReplicationPeers creates a new ListReplicationPeers request that will list replication
// peers in hbase.
//
// By default, matches all replication peers. Use the options (ListReplicationPeersRegex) to
// set non default behaviour.
func NewListReplicationPeers(ctx context.Context, opts ...func(Call) error) (
	*ListReplicationPeers, error) {
	r := &ListReplicationPeers{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		regex: ".*",
	}
	if err := applyOptions(r, opts...); err != nil {
		return nil, err
	}
	return r, nil
}

// Name returns the name of this RPC call.
func (r *ListReplicationPeers) Name() string {
	return "ListReplicationPeers"
}

// Description returns the description of this RPC call.
func (r *ListReplicationPeers) Description() string {
	return r.Name()
}

// ToProto converts the RPC into a protobuf message.
func (r *ListReplicationPeers) ToProto() proto.Message {
	return &pb.ListReplicationPeersRequest{
		Regex: proto.String(r.regex),
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (r *ListReplicationPeers) NewResponse() proto.Message {
	return &pb.ListReplicationPeersResponse{}
}
