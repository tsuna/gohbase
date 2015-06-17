// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build integration

package gohbase_test

import (
	"flag"
	"testing"
	"time"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/test"
	"golang.org/x/net/context"
)

var host = flag.String("HBase Host", "localhost", "The location where HBase is running")

func TestGet(t *testing.T) {
	table := "test1"
	key := "row1"
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	headers := map[string][]string{"cf": nil}

	c := gohbase.NewClient(host)

	err := test.CreateTable(table, []string{"cf"})
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	_, err = c.Put(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	_, err = c.Get(context.Background(), table, key, headers)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Second*0)
	_, err = c.Get(ctx, table, key, headers)
	if err != gohbase.ErrDeadline {
		t.Errorf("Get ignored the deadline")
	}

	err = test.DeleteTable(table)
	if err != nil {
		t.Errorf("Error dropping the table: %v", err)
	}
}

func TestPut(t *testing.T) {
	table := "test2"
	key := "row1"
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}

	c := gohbase.NewClient(host)

	err := test.CreateTable(table, []string{"cf"})
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	_, err = c.Put(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Second*0)
	_, err = c.Put(ctx, table, key, values)
	if err != gohbase.ErrDeadline {
		t.Errorf("Put ignored the deadline")
	}

	err = test.DeleteTable(table)
	if err != nil {
		t.Errorf("Error dropping the table: %v", err)
	}
}
