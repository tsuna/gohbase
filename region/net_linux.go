// Copyright (C) 2025 The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"context"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func tcpUserTimeoutControl(timeout time.Duration) func(
	context.Context, string, string, syscall.RawConn) error {
	return func(_ context.Context, network, address string, c syscall.RawConn) error {
		var err error
		c.Control(func(fd uintptr) {
			err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT,
				int(timeout.Milliseconds()))
		})
		return err
	}
}
