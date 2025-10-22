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

// AddReplicationPeer models a AddReplicationPeer pb call
type AddReplicationPeer struct {
	base
	tableCfs []*pb.TableCF

	peerId     string
	clusterKey string
	serial     bool
	enabled    bool
}

// AddReplicationPeerReplicateTableFamilies configures a peer to replicate the given table
// and its column families.
//
// Table must not be nil or empty.
//
// If families slice is nil or empty, all families in the table will be replicated.
func AddReplicationPeerReplicateTableFamilies(table []byte, families ...[]byte) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*AddReplicationPeer)
		if !ok {
			return errors.New("AddReplicationPeerReplicateTableFamilies option can only be used " +
				"with AddReplicationPeer")
		}
		if l.tableCfs == nil {
			return errors.New("AddReplicationPeer tableCfs is nil. Forgot to create " +
				"AddReplicationPeer with NewAddReplicationPeer() ?")
		}
		if table == nil || len(table) == 0 {
			return errors.New("table must not be nil or empty")
		}
		l.tableCfs = append(l.tableCfs,
			&pb.TableCF{
				TableName: &pb.TableName{
					// TODO: handle namespaces
					Namespace: []byte("default"),
					Qualifier: table,
				},
				Families: families,
			},
		)
		return nil
	}
}

// AddReplicationPeerSetSerial sets the serial flag on the peer config
func AddReplicationPeerSerial(serial bool) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*AddReplicationPeer)
		if !ok {
			return errors.New("AddReplicationPeerSerial option can only be used with " +
				"AddReplicationPeer")
		}
		l.serial = serial
		return nil
	}
}

// AddReplicationPeerEnabled sets whether the peer should be created as enabled or disabled
func AddReplicationPeerEnabled(enabled bool) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*AddReplicationPeer)
		if !ok {
			return errors.New("AddReplicationPeerEnabled option can only be used with " +
				"AddReplicationPeer")
		}
		l.enabled = enabled
		return nil
	}
}

// NewAddReplicationPeer creates a new AddReplicationPeer request that will create a new
// replication peer in HBase.
//
// peerId is the unique ID of the peer. Must only contain alphanumerics and underscores (_),
// no dashes (-).
//
// clusterKey is the replication sink/destination address. It must follow the format:
//
//	hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
//
// By default, creates a non-serial enabled peer that does not replicate any table. Use the options
// (AddReplicationPeerReplicateTableFamilies, AddReplicationPeerSerial, AddReplicationPeerEnabled)
// to set non default behaviour.
func NewAddReplicationPeer(ctx context.Context, peerId, clusterKey string,
	opts ...func(Call) error) (
	*AddReplicationPeer, error) {
	r := &AddReplicationPeer{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		tableCfs:   make([]*pb.TableCF, 0),
		peerId:     peerId,
		clusterKey: clusterKey,
		serial:     false,
		enabled:    true,
	}
	if err := applyOptions(r, opts...); err != nil {
		return nil, err
	}
	return r, nil
}

// Name returns the name of this RPC call.
func (r *AddReplicationPeer) Name() string {
	return "AddReplicationPeer"
}

// Description returns the description of this RPC call.
func (r *AddReplicationPeer) Description() string {
	return r.Name()
}

// ToProto converts the RPC into a protobuf message.
func (r *AddReplicationPeer) ToProto() proto.Message {
	replicationState := pb.ReplicationState_ENABLED
	if !r.enabled {
		replicationState = pb.ReplicationState_DISABLED
	}
	return &pb.AddReplicationPeerRequest{
		PeerId: proto.String(r.peerId),
		PeerConfig: &pb.ReplicationPeer{
			Clusterkey: proto.String(r.clusterKey),
			TableCfs:   r.tableCfs,
			Serial:     proto.Bool(r.serial),
			// Explicitly set ReplicateAll to false. Otherwise if tableCfs
			// contains no entries, replicateAll will be set to true on the peer
			// by hbase server.
			ReplicateAll: proto.Bool(false),
		},
		PeerState: &pb.ReplicationState{
			State: replicationState.Enum(),
		},
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (r *AddReplicationPeer) NewResponse() proto.Message {
	return &pb.AddReplicationPeerResponse{}
}
