// Copyright (C) 2025  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

//go:build !linux

package region

import (
	"context"
	"syscall"
	"time"
)

// tcpUserTimeoutControl is not supported on OS's other than linux
func tcpUserTimeoutControl(timeout time.Duration) func(
	context.Context, string, string, syscall.RawConn) error {
	return nil
}
