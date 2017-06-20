package hrpc

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
)

//Struct to represent a cluster status request
type ClusterStatus struct {
	base
}

//Create a new ClusterStatusStruct with default fields
func NewClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		base{
			ctx:   context.Background(),
			table: []byte{},
		},
	}
}

//Returns the name of the rpc function
func (c *ClusterStatus) Name() string {
	return "GetClusterStatus"
}

//Returns the Protobuf message to be sent
func (c *ClusterStatus) ToProto() (proto.Message, error) {
	return &pb.GetClusterStatusRequest{}, nil
}

//Noop
func (c *ClusterStatus) SetFamilies(fam map[string][]string) error {
	return nil
}

//Noop
func (c *ClusterStatus) SetFilter(ft filter.Filter) error {
	return nil
}

//Returns the empty protobuf response
func (c *ClusterStatus) NewResponse() proto.Message {
	return &pb.GetClusterStatusResponse{}
}
