// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"golang.org/x/net/context"
)

var (
	// ErrShortWrite is used when the writer thread only succeeds in writing
	// part of its buffer to the socket, and not all of the buffer was sent
	ErrShortWrite = errors.New("short write occurred while writing to socket")
)

// Client manages a connection to a RegionServer.
type Client struct {
	id uint32

	conn net.Conn

	// Hostname or IP address of the RegionServer.
	host string

	// Port of the RegionServer.
	port uint16

	// writeMutex is used to prevent multiple threads from writing to the
	// socket at the same time.
	writeMutex *sync.Mutex

	// sendErr is set once a write fails.
	sendErr error

	rpcs []rpcAndCtx
}

// A container struct to hold both an RPC call and its context
type rpcAndCtx struct {
	ctx context.Context
	rpc hrpc.Call
}

// NewClient creates a new RegionClient.
func NewClient(host string, port uint16) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil,
			fmt.Errorf("failed to connect to the RegionServer at %s: %s", addr, err)
	}
	c := &Client{
		conn:       conn,
		host:       host,
		port:       port,
		writeMutex: &sync.Mutex{},
	}
	err = c.sendHello()
	if err != nil {
		return nil, err
	}
	go c.processRpcs()
	return c, nil
}

func (c *Client) processRpcs() {
	for {
		//TODO: make this value configurable
		time.Sleep(time.Millisecond * 100)

		c.writeMutex.Lock()

		// If c.sendErr is set, this writer has encountered an unrecoverable
		// error. It will repeat the error it encountered to any outstanding
		// rpcs, and not attempt to send them.
		if c.sendErr != nil {
			for _, rpc := range c.rpcs {
				resch := rpc.rpc.GetResultChans()
				resch <- hrpc.RPCResult{nil, c.sendErr}
			}
			c.rpcs = nil
			c.writeMutex.Unlock()
			break
		}

		newrpcs := []rpcAndCtx{}

		for _, rpc := range c.rpcs {
			// If the deadline has been exceeded, don't bother sending the
			// request. The function that placed the RPC in our queue should
			// stop waiting for a result and return an error.
			select {
			case _, ok := <-rpc.ctx.Done():
				if !ok {
					continue
				}
			default:
			}

			resch := rpc.rpc.GetResultChans()
			msg, err := c.SendRPC(rpc.rpc)
			if isRecoverable(err) {
				newrpcs = append(newrpcs, rpc)
			} else {
				if err != nil {
					c.sendErr = err
				}
				resch <- hrpc.RPCResult{msg, err}
			}
		}
		c.rpcs = newrpcs
		c.writeMutex.Unlock()
	}
}

func isRecoverable(err error) bool {
	// TODO: identify which errors we can treat as recoverable
	if err == ErrShortWrite {
		return true
	}
	return false
}

// Sends the given buffer to the RegionServer.
func (c *Client) write(buf []byte) error {
	n, err := c.conn.Write(buf)

	if err != nil {
		// There was an error while writing
		return err
	}
	if n != len(buf) {
		// We failed to write the entire buffer
		// TODO: Perhaps handle this in another way than closing down
		return ErrShortWrite
	}
	return nil
}

// Tries to read enough data to fully fill up the given buffer.
func (c *Client) readFully(buf []byte) error {
	// TODO: Handle short reads.
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

// QueueRPC will add an rpc call to the queue for processing by the writer
// goroutine
func (c *Client) QueueRPC(ctx context.Context, rpc hrpc.Call) {
	c.writeMutex.Lock()
	c.rpcs = append(c.rpcs, rpcAndCtx{ctx, rpc})
	c.writeMutex.Unlock()
}

// SendRPC sends an RPC out to the wire.
// Returns the response (for now, as the call is synchronous).
func (c *Client) SendRPC(rpc hrpc.Call) (proto.Message, error) {
	// Header.
	c.id++
	reqheader := &pb.RequestHeader{
		CallId:       &c.id,
		MethodName:   proto.String(rpc.Name()),
		RequestParam: proto.Bool(true),
	}

	payload, err := rpc.Serialize()
	if err != nil {
		return nil, fmt.Errorf("Failed to serialize RPC: %s", err)
	}
	payloadLen := proto.EncodeVarint(uint64(len(payload)))

	headerData, err := proto.Marshal(reqheader)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal Get request: %s", err)
	}

	buf := make([]byte, 5, 4+1+len(headerData)+len(payloadLen)+len(payload))
	binary.BigEndian.PutUint32(buf, uint32(cap(buf)-4))
	buf[4] = byte(len(headerData))
	buf = append(buf, headerData...)
	buf = append(buf, payloadLen...)
	buf = append(buf, payload...)

	err = c.write(buf)
	if err != nil {
		return nil, err
	}

	var sz [4]byte
	err = c.readFully(sz[:])
	if err != nil {
		return nil, err
	}

	buf = make([]byte, binary.BigEndian.Uint32(sz[:]))
	err = c.readFully(buf)
	if err != nil {
		return nil, err
	}

	resp := &pb.ResponseHeader{}
	respLen, nb := proto.DecodeVarint(buf)
	buf = buf[nb:]
	err = proto.UnmarshalMerge(buf[:respLen], resp)
	buf = buf[respLen:]
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize the response header: %s", err)
	}
	if resp.CallId == nil {
		return nil, fmt.Errorf("Response doesn't have a call ID!")
	} else if *resp.CallId != c.id {
		return nil, fmt.Errorf("Not the callId we expected: %d", *resp.CallId)
	}

	if resp.Exception != nil {
		return nil, fmt.Errorf("remote exception %s:\n%s",
			*resp.Exception.ExceptionClassName, *resp.Exception.StackTrace)
	}

	respLen, nb = proto.DecodeVarint(buf)
	buf = buf[nb:]
	rpcResp := rpc.NewResponse()
	err = proto.UnmarshalMerge(buf, rpcResp)
	buf = buf[respLen:]

	return rpcResp, err
}
