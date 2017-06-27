package hrpc

import (
	"context"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

type CompactRegion struct {
	tableOp

	region string
	major  bool
	family []byte
}

//Create a new CompactRegion struct
func NewCompactRegion(region string, family []byte, major bool) *CompactRegion {
	chunks := strings.Split(region, ",")
	return &CompactRegion{
		tableOp: tableOp{
			base: base{
				ctx:   context.Background(),
				table: []byte(chunks[0]),
				key:   []byte(chunks[1]),
			},
		},
		region: region,
		family: family,
		major:  major,
	}
}

//Return the name of this RPC  method
func (cr *CompactRegion) Name() string {
	return "CompactRegion"
}

// ToProto converts the RPC into a protobuf message
func (cr *CompactRegion) ToProto() (proto.Message, error) {
	return &pb.CompactRegionRequest{
		Region: &pb.RegionSpecifier{
			Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte(cr.region),
		},
		Major:  &cr.major,
		Family: cr.family,
	}, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (cr *CompactRegion) NewResponse() proto.Message {
	return &pb.CompactRegionResponse{}
}
