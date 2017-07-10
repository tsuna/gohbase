// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
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
	// deserialized from the reader and returns number of bytes read or error.
	DeserializeCellBlocks(proto.Message, []byte) (uint32, error)
}

var (
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

	// javaUnrecoverableExceptions is a map where all Java exceptions that signify
	// the RPC should be sent again are listed (as keys). If a Java exception
	// listed here is returned by HBase, the RegionClient will be closed and a new
	// one should be established.
	javaUnrecoverableExceptions = map[string]struct{}{
		"org.apache.hadoop.hbase.regionserver.RegionServerAbortedException": struct{}{},
		"org.apache.hadoop.hbase.regionserver.RegionServerStoppedException": struct{}{},
		"org.apache.hadoop.hbase.regionserver.ServerNotRunningYetException": struct{}{},
	}
)

const (
	// RegionClient is a ClientType that means this will be a normal client
	RegionClient = ClientType("ClientService")

	// MasterClient is a ClientType that means this client will talk to the
	// master server
	MasterClient = ClientType("MasterService")

	// readTimeout is the maximum amount of time to wait for regionserver reply
	readTimeout = 30 * time.Second
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
	conn net.Conn

	// Address of the RegionServer.
	addr string

	// once used for concurrent calls to fail
	once sync.Once

	rpcs chan hrpc.Call
	done chan struct{}

	// sent contains the mapping of sent call IDs to RPC calls, so that when
	// a response is received it can be tied to the correct RPC
	sentM sync.Mutex // protects sent
	sent  map[uint32]hrpc.Call

	// inFlight is number of rpcs sent to regionserver awaiting response
	inFlightM sync.Mutex // protects inFlight and SetReadDeadline
	inFlight  uint32

	id uint32

	rpcQueueSize  int
	flushInterval time.Duration

	effectiveUser string
}

// QueueRPC will add an rpc call to the queue for processing by the writer goroutine
func (c *client) QueueRPC(rpc hrpc.Call) {
	if _, ok := rpc.(*hrpc.Mutate); ok && c.rpcQueueSize > 1 {
		// batch mutates
		select {
		case <-rpc.Context().Done():
			// rpc timed out before being processed
		case <-c.done:
			rpc.ResultChan() <- hrpc.RPCResult{Error: ErrClientDead}
		case c.rpcs <- rpc:
		}
	} else {
		// send the rest of rpcs right away in the same goroutine
		if err := c.trySend(rpc); err != nil {
			rpc.ResultChan() <- hrpc.RPCResult{Error: err}
		}
	}
}

// Close asks this region.Client to close its connection to the RegionServer.
// All queued and outstanding RPCs, if any, will be failed as if a connection
// error had happened.
func (c *client) Close() {
	c.fail(ErrClientDead)
}

// Addr returns address of the region server the client is connected to
func (c *client) Addr() string {
	return c.addr
}

// String returns a string represintation of the current region client
func (c *client) String() string {
	return fmt.Sprintf("RegionClient{Addr: %s}", c.addr)
}

func (c *client) inFlightUp() {
	c.inFlightM.Lock()
	c.inFlight++
	// we expect that at least the last request can be completed within readTimeout
	c.conn.SetReadDeadline(time.Now().Add(readTimeout))
	c.inFlightM.Unlock()
}

func (c *client) inFlightDown() {
	c.inFlightM.Lock()
	c.inFlight--
	// reset read timeout if we are not waiting for any responses
	// in order to prevent from closing this client if there are no request
	if c.inFlight == 0 {
		c.conn.SetReadDeadline(time.Time{})
	}
	c.inFlightM.Unlock()
}

func (c *client) fail(err error) {
	c.once.Do(func() {
		log.WithFields(log.Fields{
			"client": c,
			"err":    err,
		}).Error("error occured, closing region client")

		// we don't close c.rpcs channel to make it block in select of QueueRPC
		// and avoid dealing with synchronization of closing it while someone
		// might be sending to it. Go's GC will take care of it.

		// tell goroutines to stop
		close(c.done)

		c.failSentRPCs()
		// we close connection to the regionserver,
		// to let it know that we can't receive anymore
		c.conn.Close()
	})
}

func (c *client) failSentRPCs() {
	// channel is closed, clean up awaiting rpcs
	c.sentM.Lock()
	sent := c.sent
	c.sent = make(map[uint32]hrpc.Call)
	c.sentM.Unlock()

	log.WithFields(log.Fields{
		"client": c,
		"count":  len(sent),
	}).Debug("failing awaiting RPCs")

	// send error to awaiting rpcs
	for _, rpc := range sent {
		rpc.ResultChan() <- hrpc.RPCResult{Error: ErrClientDead}
	}
}

func (c *client) registerRPC(rpc hrpc.Call) uint32 {
	currID := atomic.AddUint32(&c.id, 1)
	c.sentM.Lock()
	c.sent[currID] = rpc
	c.sentM.Unlock()
	return currID
}

func (c *client) unregisterRPC(id uint32) hrpc.Call {
	c.sentM.Lock()
	rpc := c.sent[id]
	delete(c.sent, id)
	c.sentM.Unlock()
	return rpc
}

func (c *client) processRPCs() {
	batch := make([]hrpc.Call, 0, c.rpcQueueSize)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			batch = c.sendBatch(batch)
		case rpc := <-c.rpcs:
			batch = append(batch, rpc)
			if len(batch) == c.rpcQueueSize {
				batch = c.sendBatch(batch)
			}
		}
	}
}

func (c *client) trySend(rpc hrpc.Call) error {
	select {
	case <-c.done:
		// An unrecoverable error has occured,
		// region client has been stopped,
		// don't send rpcs
		return ErrClientDead
	case <-rpc.Context().Done():
		// If the deadline has been exceeded, don't bother sending the
		// request. The function that placed the RPC in our queue should
		// stop waiting for a result and return an error.
		return nil
	default:
		id := c.registerRPC(rpc)

		if err := c.send(id, rpc); err != nil {
			if _, ok := err.(UnrecoverableError); ok {
				c.fail(err)
			}
			if r := c.unregisterRPC(id); r != nil {
				// we are the ones to unregister the rpc,
				// return err to notify client of it
				return err
			}
		}
		return nil
	}
}

func (c *client) sendBatch(rpcs []hrpc.Call) []hrpc.Call {
	for i, rpc := range rpcs {
		if err := c.trySend(rpc); err != nil {
			rpc.ResultChan() <- hrpc.RPCResult{Error: err}
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
	b := make([]byte, size)

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
	rpc := c.unregisterRPC(callID)
	if rpc == nil {
		return fmt.Errorf("got a response with an unexpected call ID: %d", callID)
	}
	c.inFlightDown()

	// Here we know for sure that we got a response for rpc we asked
	var response proto.Message
	if header.Exception == nil {
		response = rpc.NewResponse()
		if err = buf.DecodeMessage(response); err != nil {
			rpc.ResultChan() <- hrpc.RPCResult{
				Error: fmt.Errorf("failed to decode the response: %s", err)}
			return nil
		}
		var cellsLen uint32
		if header.CellBlockMeta != nil {
			cellsLen = header.CellBlockMeta.GetLength()
		}
		if d, ok := rpc.(canDeserializeCellBlocks); cellsLen > 0 && ok {
			b := buf.Bytes()[size-cellsLen:]
			nread, err := d.DeserializeCellBlocks(response, b)
			if err != nil {
				rpc.ResultChan() <- hrpc.RPCResult{
					Error: fmt.Errorf("failed to decode the response: %s", err)}
				return nil
			}
			if int(nread) < len(b) {
				rpc.ResultChan() <- hrpc.RPCResult{
					Error: fmt.Errorf("short read: buffer len %d, read %d", len(b), nread)}
				return nil
			}
		}
	} else {
		javaClass := *header.Exception.ExceptionClassName
		err = fmt.Errorf("HBase Java exception %s: \n%s", javaClass, *header.Exception.StackTrace)
		if _, ok := javaRetryableExceptions[javaClass]; ok {
			// This is a recoverable error. The client should retry.
			err = RetryableError{err}
		} else if _, ok := javaUnrecoverableExceptions[javaClass]; ok {
			// This is unrecoverable, close the client.
			return UnrecoverableError{err}
		}
	}
	rpc.ResultChan() <- hrpc.RPCResult{Msg: response, Error: err}
	return nil
}

// write sends the given buffer to the RegionServer.
func (c *client) write(buf []byte) error {
	_, err := c.conn.Write(buf)
	return err
}

// Tries to read enough data to fully fill up the given buffer.
func (c *client) readFully(buf []byte) error {
	_, err := io.ReadFull(c.conn, buf)
	return err
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
func (c *client) send(id uint32, rpc hrpc.Call) error {
	request, err := rpc.ToProto()
	if err != nil {
		return fmt.Errorf("failed to convert RPC: %s", err)
	}

	b := newBuffer(4)
	defer func() { freeBuffer(b) }()

	buf := proto.NewBuffer(b[4:])
	buf.Reset()

	header := &pb.RequestHeader{
		CallId:       &id,
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
	c.inFlightUp()
	return nil
}
