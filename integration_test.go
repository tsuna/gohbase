// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build integration

package gohbase_test

import (
	"bytes"
	"flag"
	"fmt"
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
	headers := map[string][]string{"cf": nil}
	if host == nil {
		t.Fatal("Host is not set!")
	}

	c := gohbase.NewClient(*host)
	err := _InsertKeyValue(c, key, val)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	rsp, err := c.Get(context.Background(), table, key, headers)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	rsp_value := rsp.Result.Cell[0].GetValue()
	if !bytes.Equal(rsp_value, val) {
		t.Errorf("Get returned an incorrect result. Expected: %v, Got: %v",
			val, rsp_value)
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

func TestMultiplePutsGets(t *testing.T) {
	const num_ops = 1000
	keyPrefix := "row3"
	headers := map[string][]string{"cf": nil}
	c := gohbase.NewClient(*host)
	for i := 0; i < num_ops; i++ {
		key := keyPrefix + string(i)
		err := _InsertKeyValue(c, key, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Errorf("Put returned an error: %v", err)
		}
	}
	for i := num_ops - 1; i >= 0; i-- {
		key := keyPrefix + string(i)
		rsp, err := c.Get(context.Background(), table, key, headers)
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
		}
		rsp_value := rsp.Result.Cell[0].GetValue()
		if !bytes.Equal(rsp_value, []byte(fmt.Sprintf("%d", i))) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Got: %v",
				[]byte(fmt.Sprintf("%d", i)), rsp_value)
		}
	}
}

// Note: This function currently causes an infinite loop in the client throwing the error -
// 2015/06/19 14:34:11 Encountered an error while reading: Failed to read from the RS: EOF
func TestChangingRegionServers(t *testing.T) {
	key := "row4"
	val := []byte("1")
	headers := map[string][]string{"cf": nil}
	if host == nil {
		t.Fatal("Host is not set!")
	}
	c := gohbase.NewClient(*host)
	err := _InsertKeyValue(c, key, val)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	// RegionServer 1 hosts all the current regions.
	// Now launch servers 2,3
	test.LaunchRegionServers([]string{"2", "3"})

	// Now (gracefully) stop servers 1,2.
	// All regions should now be on server 3.
	test.StopRegionServers([]string{"1", "2"})
	rsp, err := c.Get(context.Background(), table, key, headers)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	rsp_value := rsp.Result.Cell[0].GetValue()
	if !bytes.Equal(rsp_value, val) {
		t.Errorf("Get returned an incorrect result. Expected: %v, Got: %v", val, rsp_value)
	}

	// Clean up by re-launching RS1 and closing RS3
	test.LaunchRegionServers([]string{"1"})
	test.StopRegionServers([]string{"3"})
}

func BenchmarkPut(b *testing.B) {
	b.ReportAllocs()
	keyPrefix := "row5"
	c := gohbase.NewClient(*host)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keyPrefix + string(i)
		err := _InsertKeyValue(c, key, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			b.Errorf("Put returned an error: %v", err)
		}
	}

}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	keyPrefix := "row6"
	c := gohbase.NewClient(*host)
	for i := 0; i < b.N; i++ {
		key := keyPrefix + string(i)
		err := _InsertKeyValue(c, key, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			b.Errorf("Put returned an error: %v", err)
		}
	}

	b.ResetTimer()
	headers := map[string][]string{"cf": nil}
	for i := 0; i < b.N; i++ {
		key := keyPrefix + string(i)
		c.Get(context.Background(), table, key, headers)
	}
}

// Helper function. Given a client, key, value inserts into the table under CF 'a'.
// May want to move to test/test.go but I wanted to minimize the dependencies in that file.
func _InsertKeyValue(c *gohbase.Client, key string, value []byte) error {
	values := map[string]map[string][]byte{"cf": map[string][]byte{}}
	values["cf"]["a"] = value
	_, err := c.Put(context.Background(), table, key, values)
	return err
}
