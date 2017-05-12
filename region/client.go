// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

// ClientType is a type alias to represent the type of this region client
type ClientType string

type canDeserializeCellBlocks interface {
	// DeserializeCellBlocks populates passed protobuf message with results
	// deserialized from the reader
	DeserializeCellBlocks(proto.Message, io.Reader, uint32) error
}

var (
	// ErrShortWrite is used when the writer thread only succeeds in writing
	// part of its buffer to the socket, and not all of the buffer was sent
	ErrShortWrite = errors.New("short write occurred while writing to socket")

	// ErrMissingCallID is used when HBase sends us a response message for a
	// request that we didn't send
	ErrMissingCallID = errors.New("got a response with a nonsensical call ID")

	// ErrClientDead is returned to rpcs when Close() is called or when client
	// died because of failed send or receive
	ErrClientDead = UnrecoverableError{errors.New("client is dead")}

	// javaRetryableExceptions is a map where all Java exceptions that signify
	// the RPC should be sent again are listed (as keys). If a Java exception
	// listed here is returned by HBase, the client should attempt to resend
	// the RPC message, potentially via a different region client.
	javaRetryableExceptions = map[string]struct{}{
		"org.apache.hadoop.hbase.NotServingRegionException":         struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionMovedException":   struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionOpeningException": struct{}{},
	}
)

const (
	// RegionClient is a ClientType that means this will be a normal client
	RegionClient = ClientType("ClientService")

	// MasterClient is a ClientType that means this client will talk to the
	// master server
	MasterClient = ClientType("MasterService")
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		var b []byte
		return b
	},
}

func newBuffer(size int) []byte {
	b := bufferPool.Get().([]byte)
	if cap(b) < size {
		doublecap := 2 * cap(b)
		if doublecap > size {
			return make([]byte, size, doublecap)
		}
		return make([]byte, size)
	}
	return b[:size]
}

func freeBuffer(b []byte) {
	bufferPool.Put(b[:0])
}

// UnrecoverableError is an error that this region.Client can't recover from.
// The connection to the RegionServer has to be closed and all queued and
// outstanding RPCs will be failed / retried.
type UnrecoverableError struct {
	error
}

func (e UnrecoverableError) Error() string {
	return e.error.Error()
}

// RetryableError is an error that indicates the RPC should be retried because
// the error is transient (e.g. a region being momentarily unavailable).
type RetryableError struct {
	error
}

func (e RetryableError) Error() string {
	return e.error.Error()
}

// client manages a connection to a RegionServer.
type client struct {
	conn io.ReadWriteCloser

	// Hostname or IP address of the RegionServer.
	host string

	// Port of the RegionServer.
	port uint16

	// err is set once a write or read fails.
	err  error
	errM sync.RWMutex // protects err

	rpcs chan hrpc.Call
	done chan struct{}

	// sent contains the mapping of sent call IDs to RPC calls, so that when
	// a response is received it can be tied to the correct RPC
	sent  map[uint32]hrpc.Call
	sentM sync.Mutex // protects sent

	rpcQueueSize  int
	flushInterval time.Duration

	effectiveUser string
}

type call struct {
	id uint32
	hrpc.Call
}

// QueueRPC will add an rpc call to the queue for processing by the writer
// goroutine
func (c *client) QueueRPC(rpc hrpc.Call) {
	select {
	case <-rpc.Context().Done():
		// rpc timed out before being processed
	case <-c.done:
		rpc.ResultChan() <- hrpc.RPCResult{Error: ErrClientDead}
	case c.rpcs <- rpc:
	}
}

// Close asks this region.Client to close its connection to the RegionServer.
// All queued and outstanding RPCs, if any, will be failed as if a connection
// error had happened.
func (c *client) Close() {
	c.fail(ErrClientDead)
}

// Host returns the host that this client talks to
func (c *client) Host() string {
	return c.host
}

// Port returns the port that this client talks over
func (c *client) Port() uint16 {
	return c.port
}

// String returns a string represintation of the current region client
func (c *client) String() string {
	return fmt.Sprintf("RegionClient{Host: %s, Port: %d}", c.host, c.port)
}

func (c *client) fail(err error) {
	c.errM.Lock()
	if c.err != nil {
		c.errM.Unlock()
		return
	}
	c.err = err
	c.errM.Unlock()

	log.WithFields(log.Fields{
		"client": c,
		"err":    err,
	}).Error("error occured, closing region client")

	// we don't close c.rpcs channel to make it block in select of QueueRPC
	// and avoid dealing with synchronization of closing it while someone
	// might be sending to it. Go's GC will take care of it.

	// tell goroutines to stop
	close(c.done)
	// we close connection to the regionserver,
	// to let it know that we can't receive anymore
	c.conn.Close()
}

func (c *client) failAwaitingRPCs() {
	c.errM.Lock()
	res := hrpc.RPCResult{Error: c.err}
	c.errM.Unlock()
	// channel is closed, clean up awaiting rpcs
	c.sentM.Lock()
	sent := c.sent
	c.sent = make(map[uint32]hrpc.Call)
	c.sentM.Unlock()

	log.WithFields(log.Fields{
		"client": c,
		"err":    res.Error,
		"count":  len(sent),
	}).Debug("failing awaiting RPCs")

	// send error to awaiting rpcs
	for _, rpc := range sent {
		rpc.ResultChan() <- res
	}
}

func (c *client) processRPCs() {
	batch := make([]*call, 0, c.rpcQueueSize)
	ticker := time.NewTicker(c.flushInterval)
	var currID uint32
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			// we cleanup awaiting rpcs here because otherwise we might
			// end up with unprocessed: Since go's select is randomized,
			// in case c.rpcs <- rpc in QeueueRPC is chosen over <-c.done,
			// here rpc := <-c.rpcs can still be chosen over <-c.done as
			// well resulting in an extra rpc we need to fail after an
			// error has been set
			c.failAwaitingRPCs()
			return
		case <-ticker.C:
			batch = c.sendBatch(batch)
		case rpc := <-c.rpcs:
			currID++
			// track the rpc
			c.sentM.Lock()
			c.sent[currID] = rpc
			c.sentM.Unlock()
			// associate with id and append
			batch = append(batch, &call{
				id:   currID,
				Call: rpc,
			})
			if len(batch) == c.rpcQueueSize {
				batch = c.sendBatch(batch)
			}
		}
	}
}

func (c *client) sendBatch(rpcs []*call) []*call {
	for i, rpc := range rpcs {
		select {
		case <-c.done:
			// An unrecoverable error has occured,
			// region client has been stopped,
			// don't send rpcs
			return nil
		case <-rpc.Context().Done():
			// If the deadline has been exceeded, don't bother sending the
			// request. The function that placed the RPC in our queue should
			// stop waiting for a result and return an error.
		default:
			err := c.send(rpc)
			if _, ok := err.(UnrecoverableError); ok {
				c.fail(err)
				return nil
			} else if err != nil {
				// Unexpected error, return to caller
				c.sentM.Lock()
				delete(c.sent, rpc.id)
				c.sentM.Unlock()
				rpc.ResultChan() <- hrpc.RPCResult{Error: err}
			}
		}
		// set to nil so that GC isn't blocked to clean up rpc
		rpcs[i] = nil
	}
	// reset size, but preserve capacity to reuse the slice
	return rpcs[:0]
}

func (c *client) receiveRPCs() {
	for {
		select {
		case <-c.done:
			return
		default:
			err := c.receive()
			if _, ok := err.(UnrecoverableError); ok {
				c.fail(err)
				return
			} else if err != nil {
				log.WithFields(log.Fields{
					"client": c,
					"err":    err,
				}).Errorf("error receiving rpc response")
			}
		}
	}
}

func (c *client) receive() error {
	var sz [4]byte
	err := c.readFully(sz[:])
	if err != nil {
		return UnrecoverableError{err}
	}

	size := binary.BigEndian.Uint32(sz[:])
	b := newBuffer(int(size))
	defer freeBuffer(b)

	err = c.readFully(b)
	if err != nil {
		return UnrecoverableError{err}
	}

	buf := proto.NewBuffer(b)

	var header pb.ResponseHeader
	if err = buf.DecodeMessage(&header); err != nil {
		return fmt.Errorf("failed to decode the response header: %s", err)
	}
	if header.CallId == nil {
		return ErrMissingCallID
	}

	callID := *header.CallId
	c.sentM.Lock()
	rpc, ok := c.sent[callID]
	delete(c.sent, callID)
	c.sentM.Unlock()

	if !ok {
		return fmt.Errorf("got a response with an unexpected call ID: %d", callID)
	}

	// Here we know for sure that we got a response for rpc we asked
	var response proto.Message
	if header.Exception == nil {
		response = rpc.NewResponse()
		if err = buf.DecodeMessage(response); err != nil {
			return fmt.Errorf("failed to decode the response: %s", err)
		}
		var cellsLen uint32
		if header.CellBlockMeta != nil {
			cellsLen = header.CellBlockMeta.GetLength()
		}
		if d, ok := rpc.(canDeserializeCellBlocks); cellsLen > 0 && ok {
			if err = d.DeserializeCellBlocks(
				response, bytes.NewBuffer(buf.Bytes()[size-cellsLen:]), cellsLen); err != nil {
				return UnrecoverableError{err}
			}
		}
	} else {
		javaClass := *header.Exception.ExceptionClassName
		err = fmt.Errorf("HBase Java exception %s: \n%s", javaClass, *header.Exception.StackTrace)
		if _, ok := javaRetryableExceptions[javaClass]; ok {
			// This is a recoverable error. The client should retry.
			err = RetryableError{err}
		}
	}
	rpc.ResultChan() <- hrpc.RPCResult{Msg: response, Error: err}
	return nil
}

// write sends the given buffer to the RegionServer.
func (c *client) write(buf []byte) error {
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
func (c *client) readFully(buf []byte) error {
	_, err := io.ReadFull(c.conn, buf)
	if err != nil {
		return fmt.Errorf("failed to read: %s", err)
	}
	return nil
}

// sendHello sends the "hello" message needed when opening a new connection.
func (c *client) sendHello(ctype ClientType) error {
	connHeader := &pb.ConnectionHeader{
		UserInfo: &pb.UserInformation{
			EffectiveUser: proto.String(c.effectiveUser),
		},
		ServiceName:         proto.String(string(ctype)),
		CellBlockCodecClass: proto.String("org.apache.hadoop.hbase.codec.KeyValueCodec"),
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

// send sends an RPC out to the wire.
// Returns the response (for now, as the call is synchronous).
func (c *client) send(rpc *call) error {
	request, err := rpc.ToProto()
	if err != nil {
		return fmt.Errorf("failed to convert RPC: %s", err)
	}

	b := newBuffer(4)
	defer func() { freeBuffer(b) }()

	buf := proto.NewBuffer(b[4:])
	buf.Reset()

	header := &pb.RequestHeader{
		CallId:       &rpc.id,
		MethodName:   proto.String(rpc.Name()),
		RequestParam: proto.Bool(true),
	}
	if err = buf.EncodeMessage(header); err != nil {
		return fmt.Errorf("failed to marshal request header: %s", err)
	}

	if err = buf.EncodeMessage(request); err != nil {
		return fmt.Errorf("failed to marshal request: %s", err)
	}

	payload := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(payload)))
	b = append(b[:4], payload...)

	if err = c.write(b); err != nil {
		return UnrecoverableError{err}
	}
	return nil
}
