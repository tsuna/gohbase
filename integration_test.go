// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build integration

package gohbase_test

import (
	"bytes"
	"flag"
	"os"
	"testing"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/test"
	"golang.org/x/net/context"
)

var host = flag.String("HBase Host", "localhost", "The location where HBase is running")

const table = "test1"

func TestMain(m *testing.M) {
	err := test.CreateTable(table, []string{"cf"})
	if err != nil {
		panic(err)
	}

	res := m.Run()

	err = test.DeleteTable(table)
	if err != nil {
		panic(err)
	}

	os.Exit(res)
}

func TestGet(t *testing.T) {
	key := "row1"
	val := []byte("1")
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": val}}
	headers := map[string][]string{"cf": nil}

	if host == nil {
		t.Fatal("Host is not set!")
	}

	c := gohbase.NewClient(*host)

	_, err := c.Put(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	rsp, err := c.Get(context.Background(), table, key, headers)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	if !bytes.Equal(rsp.Result.Cell[0].GetValue(), val) {
		t.Errorf("Get returned an incorrect result.")
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)
	_, err = c.Get(ctx, table, key, headers)
	if err != gohbase.ErrDeadline {
		t.Errorf("Get ignored the deadline")
	}
}

func TestPut(t *testing.T) {
	key := "row2"
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}

	if host == nil {
		t.Fatal("Host is not set!")
	}

	c := gohbase.NewClient(*host)

	_, err := c.Put(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)
	_, err = c.Put(ctx, table, key, values)
	if err != gohbase.ErrDeadline {
		t.Errorf("Put ignored the deadline")
	}
}
