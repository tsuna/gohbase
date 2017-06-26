package hrpc

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
)

// ClusterStatus to represent a cluster status request
type ClusterStatus struct {
	base
}

// NewClusterStatus creates a new ClusterStatusStruct with default fields
func NewClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		base{
			ctx:   context.Background(),
			table: []byte{},
		},
	}
}

// Name returns the name of the rpc function
func (c *ClusterStatus) Name() string {
	return "GetClusterStatus"
}

// ToProto returns the Protobuf message to be sent
func (c *ClusterStatus) ToProto() (proto.Message, error) {
	return &pb.GetClusterStatusRequest{}, nil
}

// SetFamilies is a Noop
func (c *ClusterStatus) SetFamilies(fam map[string][]string) error {
	return nil
}

// SetFilter is a no-op
func (c *ClusterStatus) SetFilter(ft filter.Filter) error {
	return nil
}

// NewResponse returns the empty protobuf response
func (c *ClusterStatus) NewResponse() proto.Message {
	return &pb.GetClusterStatusResponse{}
}
