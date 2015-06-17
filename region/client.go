// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
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

	rpcs []hrpc.Call

	// Once the rpcs list has grown to a large enough size, this channel is
	// written to to notify the writer thread that it should stop sleeping and
	// process the list
	process chan struct{}

	// sentRPCs contains the mapping of sent call IDs to RPC calls, so that when
	// a response is received it can be tied to the correct RPC
	sentRPCs      map[uint32]hrpc.Call
	sentRPCsMutex *sync.Mutex
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
		conn:          conn,
		host:          host,
		port:          port,
		writeMutex:    &sync.Mutex{},
		process:       make(chan struct{}),
		sentRPCsMutex: &sync.Mutex{},
		sentRPCs:      make(map[uint32]hrpc.Call),
	}
	err = c.sendHello()
	if err != nil {
		return nil, err
	}
	go c.processRpcs() // Writer goroutine
	go c.receiveRpcs() // Reader goroutine
	return c, nil
}

func (c *Client) processRpcs() {
	for {
		select {
		// TODO: make this value configurable
		case <-time.After(time.Millisecond * 100):
			c.writeMutex.Lock()
		case <-c.process:
			// We don't acquire the lock here, because the thread that sent
			// something on the process channel will have locked the mutex,
			// and will not release it so as to transfer ownership
		}

		// If c.sendErr is set, this writer has encountered an unrecoverable
		// error. It will repeat the error it encountered to any outstanding
		// rpcs, and not attempt to send them.
		if c.sendErr != nil {
			for _, rpc := range c.rpcs {
				resch := rpc.GetResultChan()
				resch <- hrpc.RPCResult{nil, c.sendErr}
			}
			c.rpcs = nil
			c.writeMutex.Unlock()
			continue
		}
		rpcs := make([]hrpc.Call, len(c.rpcs))
		for i, rpc := range c.rpcs {
			rpcs[i] = rpc
		}
		c.rpcs = nil
		c.writeMutex.Unlock()

		var newrpcs []hrpc.Call

		for _, rpc := range rpcs {
			// If the deadline has been exceeded, don't bother sending the
			// request. The function that placed the RPC in our queue should
			// stop waiting for a result and return an error.
			select {
			case _, ok := <-rpc.GetContext().Done():
				if !ok {
					continue
				}
			default:
			}

			err := c.sendRPC(rpc)
			if isRecoverable(err) {
				newrpcs = append(newrpcs, rpc)
			} else if err != nil {
				c.sendErr = err
				resch := rpc.GetResultChan()
				resch <- hrpc.RPCResult{nil, err}
			}
		}

		c.writeMutex.Lock()
		c.rpcs = append(c.rpcs, newrpcs...)
		c.writeMutex.Unlock()
	}
}

func (c *Client) receiveRpcs() {
	for {
		var sz [4]byte
		err := c.readFully(sz[:])
		if err != nil {
			log.Printf("Encountered an error while reading: %v\n", err)
			continue
		}

		buf := make([]byte, binary.BigEndian.Uint32(sz[:]))
		err = c.readFully(buf)
		if err != nil {
			log.Printf("Encountered an error while reading: %v\n", err)
			continue
		}

		resp := &pb.ResponseHeader{}
		respLen, nb := proto.DecodeVarint(buf)
		buf = buf[nb:]
		err = proto.UnmarshalMerge(buf[:respLen], resp)
		buf = buf[respLen:]
		if err != nil {
			log.Printf("Failed to deserialize the response header: %s\n", err)
			continue
		}
		if resp.CallId == nil {
			log.Printf("Response doesn't have a call ID!\n")
			continue
		}

		c.sentRPCsMutex.Lock()
		rpc, ok := c.sentRPCs[*resp.CallId]
		c.sentRPCsMutex.Unlock()

		if !ok {
			log.Printf("Received a response with an unexpected call ID!\n")
			continue
		}

		if resp.Exception != nil {
			// TODO: Properly handle this error
			log.Printf("remote exception %s:\n%s",
				*resp.Exception.ExceptionClassName, *resp.Exception.StackTrace)
		}

		respLen, nb = proto.DecodeVarint(buf)
		buf = buf[nb:]
		rpcResp := rpc.NewResponse()
		err = proto.UnmarshalMerge(buf, rpcResp)
		buf = buf[respLen:]

		resch := rpc.GetResultChan()
		resch <- hrpc.RPCResult{rpcResp, err}

		c.sentRPCsMutex.Lock()
		delete(c.sentRPCs, *resp.CallId)

		c.sentRPCsMutex.Unlock()
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
func (c *Client) QueueRPC(rpc hrpc.Call) {
	c.writeMutex.Lock()
	c.rpcs = append(c.rpcs, rpc)
	if len(c.rpcs) > 100 { //TODO: make configurable or pick a good number
		c.process <- struct{}{}
		// We don't release the lock here, because we want to transfer ownership
		// of the lock to the goroutine that processes the RPCs
	} else {
		c.writeMutex.Unlock()
	}
}

// sendRPC sends an RPC out to the wire.
// Returns the response (for now, as the call is synchronous).
func (c *Client) sendRPC(rpc hrpc.Call) error {
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

	c.sentRPCsMutex.Lock()
	c.sentRPCs[c.id] = rpc
	c.sentRPCsMutex.Unlock()

	return nil
}
