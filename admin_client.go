// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"fmt"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

// AdminClient to perform admistrative operations with HMaster
type AdminClient interface {
	CreateTable(t *hrpc.CreateTable) error
	DeleteTable(t *hrpc.DeleteTable) error
	EnableTable(t *hrpc.EnableTable) error
	DisableTable(t *hrpc.DisableTable) error
}

// NewAdminClient creates an admin HBase client.
func NewAdminClient(zkquorum string, options ...Option) AdminClient {
	c := newClient(zkquorum, options...)
	c.clientType = adminClient
	return c
}

func (c *client) CreateTable(t *hrpc.CreateTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.CreateTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a CreateTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) DeleteTable(t *hrpc.DeleteTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.DeleteTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a DeleteTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) EnableTable(t *hrpc.EnableTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.EnableTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a EnableTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) DisableTable(t *hrpc.DisableTable) error {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.DisableTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a DisableTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}
