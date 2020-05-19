// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/tsuna/gohbase/compression"
)

type compressor struct {
	compression.Codec
}

func min(x, y uint32) int {
	if x < y {
		return int(x)
	}
	return int(y)
}

func growBuffer(b []byte, sz int) []byte {
	l := len(b) + sz
	if l <= cap(b) {
		return b[:l]
	}
	return append(b, make([]byte, sz)...)
}

func (c *compressor) compressCellblocks(cbs net.Buffers, uncompressedLen uint32) []byte {
	b := newBuffer(4)

	// put uncompressed length
	binary.BigEndian.PutUint32(b, uncompressedLen)

	uncompressedBuffer := newBuffer(min(uncompressedLen, c.ChunkLen()))
	defer freeBuffer(uncompressedBuffer)

	var chunkLen uint32
	var lenOffset int
	for {
		n, err := cbs.Read(uncompressedBuffer)
		if n == 0 {
			break
		}

		// grow for chunk length
		lenOffset = len(b)
		b = growBuffer(b, 4)

		b, chunkLen = c.Encode(uncompressedBuffer[:n], b)

		// write the chunk length
		binary.BigEndian.PutUint32(b[lenOffset:], chunkLen)

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err) // unexpected error
		}
	}
	return b
}

func (c *compressor) decompressCellblocks(b []byte) ([]byte, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf(
			"short read on total uncompressed length: want 4 bytes, got %d", len(b))
	}
	totalUncompressedLen := binary.BigEndian.Uint32(b[:4])
	b = b[4:]

	var err error
	out := make([]byte, 0, totalUncompressedLen)
	var uncompressedSoFar uint32
	var uncompressedLen uint32
	for len(b) > 0 {
		if len(b) < 4 {
			return nil, fmt.Errorf("short read on chunk length: want 4, got %d", len(b))
		}
		compressedChunkLen := binary.BigEndian.Uint32(b)
		b = b[4:]
		if len(b) < int(compressedChunkLen) {
			return nil, fmt.Errorf("short read on chunk: want %d, got %d",
				compressedChunkLen, len(b))
		}
		out, uncompressedLen, err = c.Decode(b[:compressedChunkLen], out)
		if err != nil {
			return nil, err
		}
		// check that uncompressed lengths add up
		uncompressedSoFar += uncompressedLen
		if uncompressedSoFar > totalUncompressedLen {
			return nil, fmt.Errorf("uncompressed more than expected: expected %d, got %d so far",
				totalUncompressedLen, uncompressedSoFar)
		}
		b = b[compressedChunkLen:]
	}
	if uncompressedSoFar < totalUncompressedLen {
		return nil, fmt.Errorf("uncompressed less than expected: expected %d, got %d",
			totalUncompressedLen, uncompressedSoFar)
	}
	return out, nil
}
