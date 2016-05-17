// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"errors"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

// CreateTable represents a CreateTable HBase call
type CreateTable struct {
	tableOp

	columns    []string
	attributes map[string]string
}

// NewCreateTable creates a new CreateTable request that will create the given
// table in HBase. For use by the admin client.
func NewCreateTable(ctx context.Context, table []byte, columns []string,
	options ...func(Call) error) (*CreateTable, error) {
	ct := &CreateTable{
		tableOp: tableOp{base{
			table: table,
			ctx:   ctx,
		}},
		columns: columns,
		attributes: map[string]string{ // default attributes
			"BLOOMFILTER":         "ROW",
			"VERSIONS":            "3",
			"IN_MEMORY":           "false",
			"KEEP_DELETED_CELLS":  "FALSE",
			"DATA_BLOCK_ENCODING": "FAST_DIFF",
			"TTL":               "2147483647",
			"COMPRESSION":       "NONE",
			"MIN_VERSIONS":      "0",
			"BLOCKCACHE":        "true",
			"BLOCKSIZE":         "65536",
			"REPLICATION_SCOPE": "0",
		},
	}
	err := applyOptions(ct, options...)
	if err != nil {
		return nil, err
	}
	return ct, nil
}

// TODO: do we need to validate attributes or hbase will do it for us?

// Bloomfilter sets BLOOMFILTER attribute of column-family.
func Bloomfilter(typ string) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("Bloomfilter option can only be used with NewCreateTable.")
		}
		// TODO: validate typ
		ct.attributes["BLOOMFILTER"] = typ
		return nil
	}
}

// Versions sets VERSIONS attribute of column-family.
func Versions(n int) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("Versions option can only be used with NewCreateTable.")
		}
		// TODO: validate versions
		ct.attributes["VERSIONS"] = strconv.Itoa(n)
		return nil
	}
}

// InMemory sets IN_MEMORY attribute of column-family.
func InMemory(isInMemory bool) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("InMemory option can only be used with NewCreateTable.")
		}
		ct.attributes["IN_MEMORY"] = strconv.FormatBool(isInMemory)
		return nil
	}
}

// KeepDeletedCells sets KEEP_DELETED_CELLS attribute of column-family.
func KeepDeletedCells(keep bool) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("KeepDeletedCells option can only be used with NewCreateTable.")
		}
		ct.attributes["KEEP_DELETED_CELLS"] = strconv.FormatBool(keep)
		return nil
	}
}

// DataBlockEncoding sets DATA_BLOCK_ENCODING attribute of column-family.
func DataBlockEncoding(typ string) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("DataBlockEncoding option can only be used with NewCreateTable.")
		}
		// TODO: validate typ
		ct.attributes["DATA_BLOCK_ENCODING"] = typ
		return nil
	}
}

// TimeToLive sets TTL attribute of column-family.
func TimeToLive(seconds int) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("TimeToLive option can only be used with NewCreateTable.")
		}
		// TODO: validate seconds
		ct.attributes["TTL"] = strconv.Itoa(seconds)
		return nil
	}
}

// Compression sets COMPRESSION attribute of column-family.
func Compression(typ string) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("Compression option can only be used with NewCreateTable.")
		}
		// TODO: validate type
		ct.attributes["COMPRESSION"] = typ
		return nil
	}
}

// MinVersions sets MIN_VERSIONS attribute of column-family.
func MinVersions(n int) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("MinVersions option can only be used with NewCreateTable.")
		}
		// TODO: validate n
		ct.attributes["MIN_VERSIONS"] = strconv.Itoa(n)
		return nil
	}
}

// Blockcache sets BLOCKCACHE attribute of column-family.
func Blockcache(isBlockCache bool) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("Blockcache option can only be used with NewCreateTable.")
		}
		ct.attributes["BLOCKCACHE"] = strconv.FormatBool(isBlockCache)
		return nil
	}
}

// Blocksize sets BLOCKSIZE attribute of column-family.
func Blocksize(kb int) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("Blocksize option can only be used with NewCreateTable.")
		}
		// TODO: validate kb
		ct.attributes["BLOCKSIZE"] = strconv.Itoa(kb)
		return nil
	}
}

// ReplicationScope sets REPLICATION_SCOPE attribute of column-family.
func ReplicationScope(n int) func(Call) error {
	return func(g Call) error {
		ct, ok := g.(*CreateTable)
		if !ok {
			return errors.New("ReplicationScope option can only be used with NewCreateTable.")
		}
		// TODO: validate n
		ct.attributes["REPLICATION_SCOPE"] = strconv.Itoa(n)
		return nil
	}
}

// GetName returns the name of this RPC call.
func (ct *CreateTable) GetName() string {
	return "CreateTable"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (ct *CreateTable) Serialize() ([]byte, error) {
	pbcols := make([]*pb.ColumnFamilySchema, len(ct.columns))
	attrs := make([]*pb.BytesBytesPair, 0, len(ct.attributes))
	for key, attr := range ct.attributes {
		attrs = append(attrs, &pb.BytesBytesPair{
			First:  []byte(key),
			Second: []byte(attr),
		})
	}

	// TODO: change to be able to specify attributes per column family
	for i, col := range ct.columns {
		pbcols[i] = &pb.ColumnFamilySchema{
			Name:       []byte(col),
			Attributes: attrs,
		}
	}
	ctable := &pb.CreateTableRequest{
		TableSchema: &pb.TableSchema{
			TableName: &pb.TableName{
				Namespace: []byte("default"),
				Qualifier: ct.table,
			},
			ColumnFamilies: pbcols,
		},
	}
	return proto.Marshal(ctable)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ct *CreateTable) NewResponse() proto.Message {
	return &pb.CreateTableResponse{}
}
