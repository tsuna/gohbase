package hrpc

import (
	"context"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

type TableSchema = pb.TableSchema

// ModifyTable  represents a ModifyTable HBase call
type ModifyTable struct {
	base

	schema *TableSchema
}

// NewModifyTable create new ModifyTable object
func NewModifyTable(ctx context.Context, table []byte, schema *TableSchema) *ModifyTable {
	mt := &ModifyTable{
		base: base{
			table:    table,
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		schema: schema,
	}
	return mt
}

func (mt *ModifyTable) Name() string {
	return "ModifyTable"
}

func (mt *ModifyTable) Description() string {
	return mt.Name()
}

func (mt *ModifyTable) ToProto() proto.Message {

	namespace, table := mt.parseTableName()
	req := &pb.ModifyTableRequest{
		TableName: &pb.TableName{
			Namespace: namespace,
			Qualifier: table,
		},
	}
	mt.schema.TableName = req.TableName
	req.TableSchema = mt.schema
	return req
}

func (mt *ModifyTable) NewResponse() proto.Message {
	return &pb.ModifyTableResponse{}
}
