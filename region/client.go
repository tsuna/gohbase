// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

// Client manages a connection to a RegionServer.
type Client struct {
	id uint32

	conn net.Conn

	// Hostname or IP address of the RegionServer.
	host string

	// Port of the RegionServer.
	port uint16
}

func NewClient(host string, port uint16) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil,
			fmt.Errorf("failed to connect to the RegionServer at %s: %s", addr, err)
	}
	c := &Client{
		conn: conn,
		host: host,
		port: port,
	}
	err = c.sendHello()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Sends the given buffer to the RegionServer.
func (c *Client) write(buf []byte) error {
	n, err := c.conn.Write(buf)
	if err != nil {
		return fmt.Errorf("Failed to write to the RS: %s", err)
	} else if n != len(buf) {
		return fmt.Errorf("Failed to write everything to the RS: %s", err)
	}
	return nil
}

// Tries to read enough data to fully fill up the given buffer.
func (c *Client) readFully(buf []byte) error {
	n, err := c.conn.Read(buf)
	if err != nil {
		return fmt.Errorf("Failed to read from the RS: %s", err)
	} else if n != len(buf) {
		return fmt.Errorf("Failed to read everything from the RS: %s", err)
	}
	return nil
}

// Sends the "hello" message needed when opening a new connection.
func (c *Client) sendHello() error {
	connHeader := &pb.ConnectionHeader{
		UserInfo: &pb.UserInformation{
			EffectiveUser: proto.String("gopher"),
		},
		ServiceName: proto.String("ClientService"),
		//CellBlockCodecClass: "org.apache.hadoop.hbase.codec.KeyValueCodec",
	}
	data, err := proto.Marshal(connHeader)
	if err != nil {
		return fmt.Errorf("failed to marshal connection header: %s", err)
	}

	const header = "HBas\x00\x50" // \x50 = Simple Auth.
	buf := make([]byte, 0, len(header)+4+len(data))
	buf = append(buf, header...)
	buf = buf[:len(header)+4]
	binary.BigEndian.PutUint32(buf[6:], uint32(len(data)))
	buf = append(buf, data...)

	return c.write(buf)
}

// Sends an RPC out to the wire.
func (c *Client) SendRpc(rpc hrpc.Call) error {
	// Header.
	c.id++
	reqheader := &pb.RequestHeader{
		CallId:       &c.id,
		MethodName:   proto.String(rpc.Name()),
		RequestParam: proto.Bool(true),
	}

	payload, err := rpc.Serialize()
	if err != nil {
		return fmt.Errorf("Failed to serialize RPC: %s", err)
	}
	payloadLen := proto.EncodeVarint(uint64(len(payload)))

	headerData, err := proto.Marshal(reqheader)
	if err != nil {
		return fmt.Errorf("Failed to marshal Get request: %s", err)
	}

	buf := make([]byte, 5, 4+1+len(headerData)+len(payloadLen)+len(payload))
	binary.BigEndian.PutUint32(buf, uint32(cap(buf)-4))
	buf[4] = byte(len(headerData))
	buf = append(buf, headerData...)
	buf = append(buf, payloadLen...)
	buf = append(buf, payload...)

	err = c.write(buf)
	if err != nil {
		return err
	}

	var sz [4]byte
	err = c.readFully(sz[:])
	if err != nil {
		return err
	}

	buf = make([]byte, binary.BigEndian.Uint32(sz[:]))
	err = c.readFully(buf)
	if err != nil {
		return err
	}

	resp := &pb.ResponseHeader{}
	err = proto.UnmarshalMerge(buf[1:1+buf[0]], resp)
	if err != nil {
		return fmt.Errorf("Failed to deserialize the response header: %s", err)
	}
	if resp.CallId == nil {
		return fmt.Errorf("Response doesn't have a call ID!")
	} else if *resp.CallId != c.id {
		return fmt.Errorf("Not the callId we expected: %d", *resp.CallId)
	}
	rpcResp := rpc.NewResponse()
	err = proto.UnmarshalMerge(buf[2+buf[0]:], rpcResp)

	return nil
}
