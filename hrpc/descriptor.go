package hrpc

import (
	"context"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// GetTableDescriptors ...
type GetTableDescriptors struct {
	base

	tableNames [][]byte
}

// NewGetTableDescriptors ...
func NewGetTableDescriptors(ctx context.Context, tableNames [][]byte) *GetTableDescriptors {
	tn := &GetTableDescriptors{
		base: base{
			ctx:      ctx,
			table:    []byte{},
			resultch: make(chan RPCResult, 1),
		},
		tableNames: tableNames,
	}
	return tn
}

// Name returns the name of this RPC call.
func (gd *GetTableDescriptors) Name() string {
	return "GetTableDescriptors"
}

// Description returns the description of this RPC call.
func (gd *GetTableDescriptors) Description() string {
	return gd.Name()
}

// ToProto converts the RPC into a protobuf message.
func (gd *GetTableDescriptors) ToProto() proto.Message {
	req := &pb.GetTableDescriptorsRequest{}
	for _, tableName := range gd.tableNames {
		namespace, table := parseTableName(tableName)
		req.TableNames = append(req.TableNames, &pb.TableName{
			Namespace: namespace,
			Qualifier: table,
		})
	}
	return req
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (gd *GetTableDescriptors) NewResponse() proto.Message {
	return &pb.GetTableDescriptorsResponse{}
}
