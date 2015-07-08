// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build integration

package gohbase_test

import (
	"bytes"
	"errors"
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
	err := test.CreateTable(table, []string{"cf", "cf2"})
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
	err := insertKeyValue(c, key, "cf", val)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	rsp, err := c.Get(context.Background(), table, key, headers)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	rsp_value := rsp.Result.Cell[0].GetValue()
	if !bytes.Equal(rsp_value, val) {
		t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
			val, rsp_value)
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)
	_, err = c.Get(ctx, table, key, headers)
	if err != gohbase.ErrDeadline {
		t.Errorf("Get ignored the deadline")
	}
}

func TestGetDoesntExist(t *testing.T) {
	key := "row1.5"
	c := gohbase.NewClient(*host)
	headers := map[string][]string{"cf": nil}
	rsp, err := c.Get(context.Background(), table, key, headers)
	num_results := len(rsp.GetResult().Cell)
	if num_results != 0 {
		t.Errorf("Get expected 0 cells. Received: %d", num_results)
	}
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
}

func TestGetBadColumnFamily(t *testing.T) {
	key := "row1.625"
	c := gohbase.NewClient(*host)
	err := insertKeyValue(c, key, "cf", []byte("Bad!"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	families := map[string][]string{"badcf": nil}
	rsp, err := c.Get(context.Background(), table, key, families)
	// As of 6/24/15 this returns: rsp.GetResult() == <nil>, err == <nil>
	// and we do not handle the remote exception. Intended behavior?
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	if rsp.GetResult() != nil {
		t.Errorf("Get expected no result. Received: %v", rsp.GetResult())
	}
}

func TestGetMultipleCells(t *testing.T) {
	key := "row1.75"
	c := gohbase.NewClient(*host)
	err := insertKeyValue(c, key, "cf", []byte("cf"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	err = insertKeyValue(c, key, "cf2", []byte("cf2"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	families := map[string][]string{"cf": nil, "cf2": nil}
	rsp, err := c.Get(context.Background(), table, key, families)
	cells := rsp.GetResult().Cell
	num_results := len(cells)
	if num_results != 2 {
		t.Errorf("Get expected 2 cells. Received: %d", num_results)
	}
	for _, cell := range cells {
		if !bytes.Equal(cell.GetFamily(), cell.GetValue()) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
				cell.GetFamily(), cell.GetValue())
		}
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

func TestPutMultipleCells(t *testing.T) {
	key := "row2.5"
	values := map[string]map[string][]byte{"cf": map[string][]byte{}, "cf2": map[string][]byte{}}
	values["cf"]["a"] = []byte("a")
	values["cf"]["b"] = []byte("b")
	values["cf2"]["a"] = []byte("a")
	c := gohbase.NewClient(*host)
	_, err := c.Put(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	families := map[string][]string{"cf": nil, "cf2": nil}
	rsp, err := c.Get(context.Background(), table, key, families)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	cells := rsp.GetResult().Cell
	if len(cells) != 3 {
		t.Errorf("Get expected 3 cells. Received: %d", len(cells))
	}
	for _, cell := range cells {
		if !bytes.Equal(cell.GetQualifier(), cell.GetValue()) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
				cell.GetQualifier(), cell.GetValue())
		}
	}

}

func TestMultiplePutsGetsSequentially(t *testing.T) {
	const num_ops = 1000
	keyPrefix := "row3"
	headers := map[string][]string{"cf": nil}
	c := gohbase.NewClient(*host)
	err := performNPuts(keyPrefix, num_ops)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	for i := num_ops - 1; i >= 0; i-- {
		key := keyPrefix + fmt.Sprintf("%d", i)
		rsp, err := c.Get(context.Background(), table, key, headers)
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
		}
		rsp_value := rsp.Result.Cell[0].GetValue()
		if !bytes.Equal(rsp_value, []byte(fmt.Sprintf("%d", i))) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
				[]byte(fmt.Sprintf("%d", i)), rsp_value)
		}
	}
}

func TestMultiplePutsGetsParallel(t *testing.T) {
	const num_ops = 1000
	keyPrefix := "row3.5"
	headers := map[string][]string{"cf": nil}
	c := gohbase.NewClient(*host)
	// TODO: Currently have to CheckTable before initiating the N requests
	// 	 otherwise we face runaway client generation - one for each request.
	c.CheckTable(context.Background(), table)
	errorChan := make(chan error)
	for i := 0; i < num_ops; i++ {
		go func(client *gohbase.Client, key string) {
			err := insertKeyValue(client, key, "cf", []byte(key))
			errorChan <- err
		}(c, keyPrefix+fmt.Sprintf("%d", i))
	}
	for i := 0; i < num_ops; i++ {
		err := <-errorChan
		if err != nil {
			t.Errorf("Put returned an error: %v", err)
		}
	}
	// All puts are complete. Now do the same for gets.
	for i := num_ops - 1; i >= 0; i-- {
		go func(client *gohbase.Client, key string) {
			rsp, err := c.Get(context.Background(), table, key, headers)
			if err != nil {
				errorChan <- err
			} else {
				rsp_value := rsp.Result.Cell[0].GetValue()
				if !bytes.Equal(rsp_value, []byte(key)) {
					errorChan <- errors.New("Get returned an incorrect result.")
				}
				errorChan <- nil
			}
		}(c, keyPrefix+fmt.Sprintf("%d", i))
	}
	for i := 0; i < num_ops; i++ {
		err := <-errorChan
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
		}
	}
}

func TestTimestampIncreasing(t *testing.T) {
	key := "row4"
	c := gohbase.NewClient(*host)
	var oldTime uint64 = 0
	headers := map[string][]string{"cf": nil}
	for i := 0; i < 10; i++ {
		insertKeyValue(c, key, "cf", []byte("1"))
		rsp, err := c.Get(context.Background(), table, key, headers)
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
			break
		}
		newTime := rsp.GetResult().Cell[0].GetTimestamp()
		if newTime <= oldTime {
			t.Errorf("Timestamps are not increasing. Old Time: %v, New Time: %v",
				oldTime, newTime)
		}
		oldTime = newTime
	}
}

func TestAppend(t *testing.T) {
	key := "row7"
	c := gohbase.NewClient(*host)
	// Inserting "Hello"
	insertErr := insertKeyValue(c, key, "cf", []byte("Hello"))
	if insertErr != nil {
		t.Errorf("Put returned an error: %v", insertErr)
	}
	// Appending " my name is Dog."
	values := map[string]map[string][]byte{"cf": map[string][]byte{}}
	values["cf"]["a"] = []byte(" my name is Dog.")
	appRsp, err := c.Append(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("Append returned an error: %v", err)
	}
	if appRsp.GetResult() == nil {
		t.Errorf("Append doesn't return updated value.")
	}
	// Verifying new result is "Hello my name is Dog."
	result := appRsp.GetResult().Cell[0].GetValue()
	if !bytes.Equal([]byte("Hello my name is Dog."), result) {
		t.Errorf("Append returned an incorrect result. Expected: %v, Receieved: %v",
			[]byte("Hello my name is Dog."), result)
	}

	// Make sure the change was actually committed.
	headers := map[string][]string{"cf": nil}
	rsp, err := c.Get(context.Background(), table, key, headers)
	cells := rsp.GetResult().Cell
	if len(cells) != 1 {
		t.Errorf("Get expected 1 cells. Received: %d", len(cells))
	}
	result = cells[0].GetValue()
	if !bytes.Equal([]byte("Hello my name is Dog."), result) {
		t.Errorf("Append returned an incorrect result. Expected: %v, Receieved: %v",
			[]byte("Hello my name is Dog."), result)
	}
}

// Note: This function currently causes an infinite loop in the client throwing the error -
// 2015/06/19 14:34:11 Encountered an error while reading: Failed to read from the RS: EOF
func TestChangingRegionServers(t *testing.T) {
	key := "row8"
	val := []byte("1")
	headers := map[string][]string{"cf": nil}
	if host == nil {
		t.Fatal("Host is not set!")
	}
	c := gohbase.NewClient(*host)
	err := insertKeyValue(c, key, "cf", val)
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
		t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
			val, rsp_value)
	}

	// Clean up by re-launching RS1 and closing RS3
	test.LaunchRegionServers([]string{"1"})
	test.StopRegionServers([]string{"3"})
}

func BenchmarkPut(b *testing.B) {
	b.ReportAllocs()
	keyPrefix := "row9"
	err := performNPuts(keyPrefix, b.N)
	if err != nil {
		b.Errorf("Put returned an error: %v", err)
	}
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	keyPrefix := "row10"
	err := performNPuts(keyPrefix, b.N)
	if err != nil {
		b.Errorf("Put returned an error: %v", err)
	}
	c := gohbase.NewClient(*host)
	b.ResetTimer()
	headers := map[string][]string{"cf": nil}
	for i := 0; i < b.N; i++ {
		key := keyPrefix + fmt.Sprintf("%d", i)
		c.Get(context.Background(), table, key, headers)
	}
}

// Helper function. Given a key_prefix, num_ops, performs num_ops.
func performNPuts(keyPrefix string, num_ops int) error {
	c := gohbase.NewClient(*host)
	for i := 0; i < num_ops; i++ {
		key := keyPrefix + fmt.Sprintf("%d", i)
		err := insertKeyValue(c, key, "cf", []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			return err
		}
	}
	return nil
}

// Helper function. Given a client, key, columnFamily, value inserts into the table under column 'a'
func insertKeyValue(c *gohbase.Client, key, columnFamily string, value []byte) error {
	values := map[string]map[string][]byte{columnFamily: map[string][]byte{}}
	values[columnFamily]["a"] = value
	_, err := c.Put(context.Background(), table, key, values)
	return err
}
