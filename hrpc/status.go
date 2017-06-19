package hrpc

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
)

type ClusterStatus struct {
	base
}

func NewClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		base{
			ctx:   context.Background(),
			table: []byte{},
		},
	}
}

func (c *ClusterStatus) Name() string {
	return "GetClusterStatus"
}

func (c *ClusterStatus) ToProto() (proto.Message, error) {
	return &pb.GetClusterStatusRequest{}, nil
}

func (c *ClusterStatus) SetFamilies(fam map[string][]string) error {
	return nil
}

func (c *ClusterStatus) SetFilter(ft filter.Filter) error {
	return nil
}

func (c *ClusterStatus) NewResponse() proto.Message {
	return &pb.GetClusterStatusResponse{}
}
