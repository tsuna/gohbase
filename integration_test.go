// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

//go:build integration

package gohbase_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"google.golang.org/protobuf/proto"
)

var (
	host      = flag.String("host", "localhost", "The location where HBase is running")
	keySplits = [][]byte{[]byte("REVTEST-100"), []byte("REVTEST-200"), []byte("REVTEST-300")}
	table     string
)

func init() {
	table = fmt.Sprintf("gohbase_test_%d", time.Now().UnixNano())
}

const scannerLease = 5 * time.Second

// CreateTable creates the given table with the given families
func CreateTable(client gohbase.AdminClient, table string, cFamilies []string) error {
	// If the table exists, delete it
	DeleteTable(client, table)
	// Don't check the error, since one will be returned if the table doesn't
	// exist

	cf := make(map[string]map[string]string, len(cFamilies))
	for _, f := range cFamilies {
		cf[f] = nil
	}

	// pre-split table for reverse scan test of region changes
	ct := hrpc.NewCreateTable(context.Background(), []byte(table), cf, hrpc.SplitKeys(keySplits))
	if err := client.CreateTable(ct); err != nil {
		return err
	}

	return nil
}

// DeleteTable finds the HBase shell via the HBASE_HOME environment variable,
// and disables and drops the given table
func DeleteTable(client gohbase.AdminClient, table string) error {
	dit := hrpc.NewDisableTable(context.Background(), []byte(table))
	err := client.DisableTable(dit)
	if err != nil {
		if !strings.Contains(err.Error(), "TableNotEnabledException") {
			return err
		}
	}

	det := hrpc.NewDeleteTable(context.Background(), []byte(table))
	err = client.DeleteTable(det)
	if err != nil {
		return err
	}
	return nil
}

// LocalRegionServersCmd can be used to start or stop new local regionservers on the cluster
// (run in pseudo-distributed mode). If starting up new regionservers, the test caller is
// responsible for stopping those regionservers in the test cleanup.
// Do not use t.Parallel() if using this.
func LocalRegionServersCmd(t *testing.T, action string, servers []string) {
	hh := os.Getenv("HBASE_HOME")
	args := append([]string{action}, servers...)
	if err := exec.Command(hh+"/bin/local-regionservers.sh", args...).Run(); err != nil {
		t.Errorf("failed to %s RS=%v: %v", action, servers, err)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	if host == nil {
		panic("Host is not set!")
	}

	slog.SetLogLoggerLevel(slog.LevelDebug)

	ac := gohbase.NewAdminClient(*host)

	var err error
	for {
		err = CreateTable(ac, table, []string{"cf", "cf1", "cf2"})
		if err != nil &&
			(strings.Contains(err.Error(), "org.apache.hadoop.hbase.PleaseHoldException") ||
				strings.Contains(err.Error(),
					"org.apache.hadoop.hbase.ipc.ServerNotRunningYetException")) {
			time.Sleep(time.Second)
			continue
		} else if err != nil {
			panic(err)
		} else {
			break
		}
	}
	res := m.Run()
	err = DeleteTable(ac, table)
	if err != nil {
		panic(err)
	}

	os.Exit(res)
}

// Test retrieval of cluster status
func TestClusterStatus(t *testing.T) {
	ac := gohbase.NewAdminClient(*host)

	stats, err := ac.ClusterStatus()
	if err != nil {
		t.Fatal(err)
	}

	//Sanity check the data coming back
	if len(stats.GetMaster().GetHostName()) == 0 {
		t.Fatal("Master hostname is empty in ClusterStatus")
	}
}

func TestGet(t *testing.T) {
	key := "row1"
	val := []byte("1")
	headers := map[string][]string{"cf": nil}
	if host == nil {
		t.Fatal("Host is not set!")
	}

	c := gohbase.NewClient(*host)
	defer c.Close()
	err := insertKeyValue(c, key, "cf", val)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	rsp_value := rsp.Cells[0].Value
	if !bytes.Equal(rsp_value, val) {
		t.Errorf("Get returned an incorrect result. Expected: %v, Got: %v",
			val, rsp_value)
	}

	get.ExistsOnly()
	rsp, err = c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	} else if !*rsp.Exists {
		t.Error("Get claimed that our row didn't exist")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	get, err = hrpc.NewGetStr(ctx, table, key, hrpc.Families(headers))
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	_, err = c.Get(get)
	if err != context.DeadlineExceeded {
		t.Errorf("Get ignored the deadline")
	}
}

func TestGetDoesntExist(t *testing.T) {
	key := "row1.5"
	c := gohbase.NewClient(*host)
	defer c.Close()
	headers := map[string][]string{"cf": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	} else if results := len(rsp.Cells); results != 0 {
		t.Errorf("Get expected 0 cells. Received: %d", results)
	}

	get.ExistsOnly()
	rsp, err = c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	} else if *rsp.Exists {
		t.Error("Get claimed that our non-existent row exists")
	}
}

func TestMutateGetTableNotFound(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	key := "whatever"
	table := "NonExistentTable"
	headers := map[string][]string{"cf": nil}
	get, err := hrpc.NewGetStr(context.Background(),
		table, key, hrpc.Families(headers))
	if err != nil {
		t.Fatalf("NewGetStr returned an error: %v", err)
	}
	_, err = c.Get(get)
	if err != gohbase.TableNotFound {
		t.Errorf("Get returned unexpected error: %v", err)
	}
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		t.Fatalf("NewPutStr returned an error: %v", err)
	}
	_, err = c.Put(putRequest)
	if err != gohbase.TableNotFound {
		t.Errorf("Put returned an unexpected error: %v", err)
	}
}

func TestGetBadColumnFamily(t *testing.T) {
	key := "row1.625"
	c := gohbase.NewClient(*host)
	defer c.Close()
	err := insertKeyValue(c, key, "cf", []byte("Bad!"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	families := map[string][]string{"badcf": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(families))
	rsp, err := c.Get(get)
	if err == nil {
		t.Errorf("Get didn't return an error! (It should have)")
	}
	if rsp != nil {
		t.Errorf("Get expected no result. Received: %v", rsp)
	}
}

func TestGetMultipleCells(t *testing.T) {
	key := "row1.75"
	c := gohbase.NewClient(*host, gohbase.FlushInterval(time.Millisecond*2))
	defer c.Close()
	err := insertKeyValue(c, key, "cf", []byte("cf"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	err = insertKeyValue(c, key, "cf2", []byte("cf2"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	families := map[string][]string{"cf": nil, "cf2": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(families))
	rsp, err := c.Get(get)
	cells := rsp.Cells
	num_results := len(cells)
	if num_results != 2 {
		t.Errorf("Get expected 2 cells. Received: %d", num_results)
	}
	for _, cell := range cells {
		if !bytes.Equal(cell.Family, cell.Value) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
				cell.Family, cell.Value)
		}
	}
}

func TestGetNonDefaultNamespace(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	get, err := hrpc.NewGetStr(context.Background(), "hbase:namespace", "default")
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	rsp, err := c.Get(get)
	if err != nil {
		t.Fatalf("Get returned an error: %v", err)
	}
	if !bytes.Equal(rsp.Cells[0].Family, []byte("info")) {
		t.Errorf("Got unexpected column family: %q", rsp.Cells[0].Family)
	}
}

func TestPut(t *testing.T) {
	values := map[string]map[string][]byte{"cf": {"a": []byte("1")}}
	if host == nil {
		t.Fatal("Host is not set!")
	}

	expectNoErr := func(t *testing.T, err error) {}

	tests := []struct {
		name      string
		keylen    int
		expectErr func(t *testing.T, err error)
	}{
		{
			name:      "Normal",
			keylen:    10,
			expectErr: nil,
		},
		{
			// Test that we can insert a row of len = MAX_ROW_LENGTH
			name:      "MaxRowLength",
			keylen:    math.MaxInt16,
			expectErr: expectNoErr,
		},
		{
			name:   "RowTooLong",
			keylen: math.MaxInt16 + 1,
			expectErr: func(t *testing.T, err error) {
				javaException := "java.io.IOException: Row length 32768 is > 32767"
				if err != nil && strings.Contains(err.Error(), javaException) {
					return
				}
				t.Errorf("expected err=%q, got err=%v", javaException, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// It is important for some of these tests to be *100% isolated* to verify that we can
			// first locate the region and then perform the put.
			c := gohbase.NewClient(*host)
			defer c.Close()

			key := make([]byte, tc.keylen)
			_, err := rand.Read(key)
			if err != nil {
				t.Fatalf("Failed to generate random key: %v", err)
			}

			putRequest, err := hrpc.NewPut(context.Background(), []byte(table), key, values)
			if err != nil {
				t.Errorf("NewPutStr returned an error: %v", err)
			}
			_, err = c.Put(putRequest)
			expectNoErr(t, err)
		})
	}
}

func TestPutWithTimeout(t *testing.T) {
	key := "row2"
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	if host == nil {
		t.Fatal("Host is not set!")
	}
	c := gohbase.NewClient(*host)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	putRequest, err := hrpc.NewPutStr(ctx, table, key, values)
	_, err = c.Put(putRequest)
	if err != context.DeadlineExceeded {
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
	defer c.Close()
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	families := map[string][]string{"cf": nil, "cf2": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(families))
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	cells := rsp.Cells
	if len(cells) != 3 {
		t.Errorf("Get expected 3 cells. Received: %d", len(cells))
	}
	for _, cell := range cells {
		if !bytes.Equal(cell.Qualifier, cell.Value) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
				cell.Qualifier, cell.Value)
		}
	}

}

func TestMultiplePutsGetsSequentially(t *testing.T) {
	const num_ops = 100
	keyPrefix := "row3"
	headers := map[string][]string{"cf": nil}
	c := gohbase.NewClient(*host, gohbase.FlushInterval(time.Millisecond))
	defer c.Close()
	err := performNPuts(keyPrefix, num_ops)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	for i := num_ops - 1; i >= 0; i-- {
		key := keyPrefix + fmt.Sprintf("%d", i)
		get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
		rsp, err := c.Get(get)
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
		}
		if len(rsp.Cells) != 1 {
			t.Errorf("Incorrect number of cells returned by Get: %d", len(rsp.Cells))
		}
		rsp_value := rsp.Cells[0].Value
		if !bytes.Equal(rsp_value, []byte(fmt.Sprintf("%d", i))) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Got: %v",
				[]byte(fmt.Sprintf("%d", i)), rsp_value)
		}
	}
}

func TestMultiplePutsGetsParallel(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	const n = 1000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		wg.Add(1)
		go func() {
			if err := insertKeyValue(c, key, "cf", []byte(key)); err != nil {
				t.Error(key, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// All puts are complete. Now do the same for gets.
	headers := map[string][]string{"cf": []string{"a"}}
	for i := n - 1; i >= 0; i-- {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
			if err != nil {
				t.Error(key, err)
				return
			}
			rsp, err := c.Get(get)
			if err != nil {
				t.Error(key, err)
				return
			}
			if len(rsp.Cells) == 0 {
				t.Error(key, " got zero cells")
				return
			}
			rsp_value := rsp.Cells[0].Value
			if !bytes.Equal(rsp_value, []byte(key)) {
				t.Errorf("expected %q, got %q", key, rsp_value)
			}
		}()
	}
	wg.Wait()
}

func TestTimestampIncreasing(t *testing.T) {
	key := "row4"
	c := gohbase.NewClient(*host)
	defer c.Close()
	var oldTime uint64 = 0
	headers := map[string][]string{"cf": nil}
	for i := 0; i < 10; i++ {
		insertKeyValue(c, key, "cf", []byte("1"))
		get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
		rsp, err := c.Get(get)
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
			break
		}
		newTime := *rsp.Cells[0].Timestamp
		if newTime <= oldTime {
			t.Errorf("Timestamps are not increasing. Old Time: %v, New Time: %v",
				oldTime, newTime)
		}
		oldTime = newTime
		time.Sleep(time.Millisecond)
	}
}

func TestPutTimestamp(t *testing.T) {
	key := "TestPutTimestamp"
	c := gohbase.NewClient(*host)
	defer c.Close()
	var putTs uint64 = 50
	timestamp := time.Unix(0, int64(putTs*1e6))
	err := insertKeyValue(c, key, "cf", []byte("1"), hrpc.Timestamp(timestamp))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	get, err := hrpc.NewGetStr(context.Background(), table, key,
		hrpc.Families(map[string][]string{"cf": nil}))
	rsp, err := c.Get(get)
	if err != nil {
		t.Fatalf("Get failed: %s", err)
	}
	getTs := *rsp.Cells[0].Timestamp
	if getTs != putTs {
		t.Errorf("Timestamps are not the same. Put Time: %v, Get Time: %v",
			putTs, getTs)
	}
}

// TestDelete preps state with two column families, cf1 and cf2,
// each having 3 versions at timestamps 50, 51, 52
func TestDelete(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	ts := uint64(50)

	tests := []struct {
		in  func(string) (*hrpc.Mutate, error)
		out []*hrpc.Cell
	}{
		{
			// delete at the second version
			in: func(key string) (*hrpc.Mutate, error) {
				return hrpc.NewDelStr(context.Background(), table, key,
					map[string]map[string][]byte{"cf1": map[string][]byte{"a": nil}},
					hrpc.TimestampUint64(ts+1))
			},
			// should delete everything at and before the delete timestamp
			out: []*hrpc.Cell{
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 1),
					Value:     []byte("v2"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
			},
		},
		{
			// delete at the second version
			in: func(key string) (*hrpc.Mutate, error) {
				return hrpc.NewDelStr(context.Background(), table, key,
					map[string]map[string][]byte{"cf1": map[string][]byte{"a": nil}},
					hrpc.TimestampUint64(ts+1), hrpc.DeleteOneVersion())
			},
			// should delete only the second version
			out: []*hrpc.Cell{
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 1),
					Value:     []byte("v2"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
			},
		},
		{
			// delete the cf1 at and before ts + 1
			in: func(key string) (*hrpc.Mutate, error) {
				return hrpc.NewDelStr(context.Background(), table, key,
					map[string]map[string][]byte{"cf1": nil},
					hrpc.TimestampUint64(ts+1))
			},
			// should leave cf2 untouched
			out: []*hrpc.Cell{
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 1),
					Value:     []byte("v2"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
			},
		},
		{
			// delete the whole cf1 and qualifer a in cf2
			in: func(key string) (*hrpc.Mutate, error) {
				return hrpc.NewDelStr(context.Background(), table, key,
					map[string]map[string][]byte{
						"cf1": nil,
						"cf2": map[string][]byte{
							"a": nil,
						},
					})
			},
			out: []*hrpc.Cell{
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
			},
		},
		{
			// delete only version at ts for all qualifiers of cf1
			in: func(key string) (*hrpc.Mutate, error) {
				return hrpc.NewDelStr(context.Background(), table, key,
					map[string]map[string][]byte{
						"cf1": nil,
					}, hrpc.TimestampUint64(ts), hrpc.DeleteOneVersion())
			},
			out: []*hrpc.Cell{
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 1),
					Value:     []byte("v2"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 1),
					Value:     []byte("v2"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(ts),
					Value:     []byte("v1"),
				},
			},
		},
		{
			// delete the whole row
			in: func(key string) (*hrpc.Mutate, error) {
				return hrpc.NewDelStr(context.Background(), table, key, nil)
			},
			out: nil,
		},
		{
			// delete the whole row at ts
			in: func(key string) (*hrpc.Mutate, error) {
				return hrpc.NewDelStr(context.Background(), table, key, nil,
					hrpc.TimestampUint64(ts+1))
			},
			out: []*hrpc.Cell{
				&hrpc.Cell{
					Family:    []byte("cf1"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
				&hrpc.Cell{
					Family:    []byte("cf2"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(ts + 2),
					Value:     []byte("v3"),
				},
			},
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			key := t.Name()

			// insert three versions
			prep := func(cf string) {
				if err := insertKeyValue(c, key, cf, []byte("v1"),
					hrpc.TimestampUint64(ts)); err != nil {
					t.Fatal(err)
				}

				if err := insertKeyValue(c, key, cf, []byte("v2"),
					hrpc.TimestampUint64(ts+1)); err != nil {
					t.Fatal(err)
				}

				if err := insertKeyValue(c, key, cf, []byte("v3"),
					hrpc.TimestampUint64(ts+2)); err != nil {
					t.Fatal(err)
				}

				// insert b
				values := map[string]map[string][]byte{cf: map[string][]byte{
					"b": []byte("v1"),
				}}
				put, err := hrpc.NewPutStr(context.Background(), table, key, values,
					hrpc.TimestampUint64(ts))
				if err != nil {
					t.Fatal(err)
				}
				if _, err = c.Put(put); err != nil {
					t.Fatal(err)
				}
			}

			prep("cf1")
			prep("cf2")

			delete, err := tcase.in(key)
			if err != nil {
				t.Fatal(err)
			}

			_, err = c.Delete(delete)
			if err != nil {
				t.Fatal(err)
			}

			get, err := hrpc.NewGetStr(context.Background(), table, key,
				hrpc.MaxVersions(math.MaxInt32))
			if err != nil {
				t.Fatal(err)
			}

			rsp, err := c.Get(get)
			if err != nil {
				t.Fatal(err)
			}

			for _, c := range tcase.out {
				c.Row = []byte(t.Name())
				c.CellType = pb.CellType_PUT.Enum()
			}

			if !reflect.DeepEqual(tcase.out, rsp.Cells) {
				t.Fatalf("expected %v, got %v", tcase.out, rsp.Cells)
			}
		})
	}
}

func TestGetTimeRangeVersions(t *testing.T) {
	key := "TestGetTimeRangeVersions"
	c := gohbase.NewClient(*host)
	defer c.Close()
	err := insertKeyValue(c, key, "cf", []byte("1"), hrpc.Timestamp(time.Unix(0, 50*1e6)))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key, "cf", []byte("1"), hrpc.Timestamp(time.Unix(0, 51*1e6)))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key, "cf", []byte("1"), hrpc.Timestamp(time.Unix(0, 49*1e6)))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}

	var maxVersions uint32 = 2
	get, err := hrpc.NewGetStr(context.Background(), table, key,
		hrpc.Families(map[string][]string{"cf": nil}), hrpc.TimeRange(time.Unix(0, 0),
			time.Unix(0, 51*1e6)), hrpc.MaxVersions(maxVersions))
	rsp, err := c.Get(get)
	if err != nil {
		t.Fatalf("Get failed: %s", err)
	}
	if uint32(len(rsp.Cells)) != maxVersions {
		t.Fatalf("Expected versions: %d, Got versions: %d", maxVersions, len(rsp.Cells))
	}
	getTs1 := *rsp.Cells[0].Timestamp
	if getTs1 != 50 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			50, getTs1)
	}
	getTs2 := *rsp.Cells[1].Timestamp
	if getTs2 != 49 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			49, getTs2)
	}

	// get with no versions set
	get, err = hrpc.NewGetStr(context.Background(), table, key,
		hrpc.Families(map[string][]string{"cf": nil}), hrpc.TimeRange(time.Unix(0, 0),
			time.Unix(0, 51*1e6)))
	rsp, err = c.Get(get)
	if err != nil {
		t.Fatalf("Get failed: %s", err)
	}
	if uint32(len(rsp.Cells)) != 1 {
		t.Fatalf("Expected versions: %d, Got versions: %d", 1, len(rsp.Cells))
	}
	getTs1 = *rsp.Cells[0].Timestamp
	if getTs1 != 50 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			50, getTs1)
	}
}

func TestScanTimeRangeVersions(t *testing.T) {
	key := "TestScanTimeRangeVersions"
	c := gohbase.NewClient(*host)
	defer c.Close()
	err := insertKeyValue(c, key+"1", "cf", []byte("1"), hrpc.Timestamp(time.Unix(0, 50*1e6)))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key+"1", "cf", []byte("1"), hrpc.Timestamp(time.Unix(0, 51*1e6)))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key+"2", "cf", []byte("1"), hrpc.Timestamp(time.Unix(0, 51*1e6)))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key+"2", "cf", []byte("1"), hrpc.Timestamp(time.Unix(0, 52*1e6)))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}

	var maxVersions uint32 = 2
	scan, err := hrpc.NewScanRangeStr(context.Background(), table,
		"TestScanTimeRangeVersions1", "TestScanTimeRangeVersions3",
		hrpc.Families(map[string][]string{"cf": nil}), hrpc.TimeRange(time.Unix(0, 50*1e6),
			time.Unix(0, 53*1e6)), hrpc.MaxVersions(maxVersions))
	if err != nil {
		t.Fatalf("Scan req failed: %s", err)
	}

	var rsp []*hrpc.Result
	scanner := c.Scan(scan)
	for {
		res, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rsp = append(rsp, res)
	}

	if len(rsp) != 2 {
		t.Fatalf("Expected rows: %d, Got rows: %d", maxVersions, len(rsp))
	}
	if uint32(len(rsp[0].Cells)) != maxVersions {
		t.Fatalf("Expected versions: %d, Got versions: %d", maxVersions, len(rsp[0].Cells))
	}
	scan1 := rsp[0].Cells[0]
	if string(scan1.Row) != "TestScanTimeRangeVersions1" && *scan1.Timestamp != 51 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			51, *scan1.Timestamp)
	}
	scan2 := rsp[0].Cells[1]
	if string(scan2.Row) != "TestScanTimeRangeVersions1" && *scan2.Timestamp != 50 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			50, *scan2.Timestamp)
	}
	if uint32(len(rsp[1].Cells)) != maxVersions {
		t.Fatalf("Expected versions: %d, Got versions: %d", maxVersions, len(rsp[1].Cells))
	}
	scan3 := rsp[1].Cells[0]
	if string(scan3.Row) != "TestScanTimeRangeVersions2" && *scan3.Timestamp != 52 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			52, *scan3.Timestamp)
	}
	scan4 := rsp[1].Cells[1]
	if string(scan4.Row) != "TestScanTimeRangeVersions2" && *scan4.Timestamp != 51 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			51, *scan4.Timestamp)
	}

	// scan with no versions set
	scan, err = hrpc.NewScanRangeStr(context.Background(), table,
		"TestScanTimeRangeVersions1", "TestScanTimeRangeVersions3",
		hrpc.Families(map[string][]string{"cf": nil}), hrpc.TimeRange(time.Unix(0, 50*1e6),
			time.Unix(0, 53*1e6)),
		hrpc.NumberOfRows(1)) // set number of rows to 1 to also check that we are doing fetches
	if err != nil {
		t.Fatalf("Scan req failed: %s", err)
	}

	rsp = nil
	scanner = c.Scan(scan)
	for {
		res, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rsp = append(rsp, res)
	}
	if len(rsp) != 2 {
		t.Fatalf("Expected rows: %d, Got rows: %d", 2, len(rsp))
	}
	if len(rsp[0].Cells) != 1 {
		t.Fatalf("Expected versions: %d, Got versions: %d", 2, len(rsp[0].Cells))
	}
	if len(rsp[1].Cells) != 1 {
		t.Fatalf("Expected versions: %d, Got versions: %d", 2, len(rsp[0].Cells))
	}
}

func TestScanWithScanMetrics(t *testing.T) {
	var (
		key          = "TestScanWithScanMetrics"
		now          = time.Now()
		r1           = fmt.Sprintf("%s_%d", key, 1)
		r2           = fmt.Sprintf("%s_%d", key, 2)
		r3           = fmt.Sprintf("%s_%d", key, 3)
		val          = []byte("1")
		family       = "cf"
		ctx          = context.Background()
		rowsScanned  = "ROWS_SCANNED"
		rowsFiltered = "ROWS_FILTERED"
	)

	c := gohbase.NewClient(*host)
	defer c.Close()

	for _, r := range []string{r1, r2, r3} {
		err := insertKeyValue(c, r, family, val, hrpc.Timestamp(now))
		if err != nil {
			t.Fatalf("Put failed: %s", err)
		}
	}

	tcases := []struct {
		description          string
		filters              func(call hrpc.Call) error
		expectedRowsScanned  int64
		expectedRowsFiltered int64
		noScanMetrics        bool
	}{
		{
			description:          "scan metrics not enabled",
			expectedRowsScanned:  0,
			expectedRowsFiltered: 0,
			noScanMetrics:        true,
		},
		{
			description:          "2 rows scanned",
			expectedRowsScanned:  2,
			expectedRowsFiltered: 0,
		},
		{
			description:          "1 row scanned 1 row filtered",
			filters:              hrpc.Filters(filter.NewPrefixFilter([]byte(r1))),
			expectedRowsScanned:  1,
			expectedRowsFiltered: 1,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.description, func(t *testing.T) {
			var (
				scan *hrpc.Scan
				err  error
			)
			if tc.noScanMetrics {
				scan, err = hrpc.NewScanRangeStr(ctx, table, r1, r3)
			} else if tc.filters == nil {
				scan, err = hrpc.NewScanRangeStr(ctx, table, r1, r3, hrpc.TrackScanMetrics())
			} else {
				scan, err = hrpc.NewScanRangeStr(ctx, table, r1, r3, hrpc.TrackScanMetrics(),
					tc.filters)
			}
			if err != nil {
				t.Fatalf("Scan req failed: %s", err)
			}

			var results []*hrpc.Result
			scanner := c.Scan(scan)
			for {
				var r *hrpc.Result
				r, err = scanner.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				results = append(results, r)
			}

			actualMetrics := scanner.GetScanMetrics()

			if tc.noScanMetrics && actualMetrics != nil {
				t.Fatalf("Expected nil scan metrics, got %v", actualMetrics)
			}

			scanned := actualMetrics[rowsScanned]
			if tc.expectedRowsScanned != scanned {
				t.Errorf("Did not get expected rows scanned - expected: %d, actual %d",
					tc.expectedRowsScanned, scanned)
			}

			filtered := actualMetrics[rowsFiltered]
			if tc.expectedRowsFiltered != filtered {
				t.Errorf("Did not get expected rows filtered - expected: %d, actual %d",
					tc.expectedRowsFiltered, filtered)
			}
		})
	}

}

func TestPutTTL(t *testing.T) {
	key := "TestPutTTL"
	c := gohbase.NewClient(*host)
	defer c.Close()

	var ttl = 2 * time.Second

	err := insertKeyValue(c, key, "cf", []byte("1"), hrpc.TTL(ttl))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}

	// Wait ttl duration and try to get the value
	time.Sleep(ttl)

	get, err := hrpc.NewGetStr(context.Background(), table, key,
		hrpc.Families(map[string][]string{"cf": nil}))

	// Make sure we don't get a result back
	res, err := c.Get(get)
	if err != nil {
		t.Fatalf("Get failed: %s", err)
	}

	if len(res.Cells) > 0 {
		t.Errorf("TTL did not expire row. Expected 0 cells, got: %d", len(res.Cells))
	}
}

func checkResultRow(t *testing.T, res *hrpc.Result, expectedRow string, err, expectedErr error) {
	if err != expectedErr {
		t.Fatalf("Expected error %v, got error %v", expectedErr, err)
	}
	if len(expectedRow) > 0 && res != nil && len(res.Cells) > 0 {
		got := string(res.Cells[0].Row)
		if got != expectedRow {
			t.Fatalf("Expected row %s, got row %s", expectedRow, got)
		}
	} else if len(expectedRow) == 0 && res != nil {
		t.Fatalf("Expected no result, got %+v", *res)
	}
}

func TestScannerClose(t *testing.T) {
	key := t.Name()
	c := gohbase.NewClient(*host)
	defer c.Close()

	err := insertKeyValue(c, key+"1", "cf", []byte("1"))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key+"2", "cf", []byte("1"))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key+"3", "cf", []byte("1"))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}

	scan, err := hrpc.NewScanRangeStr(context.Background(), table,
		key+"1", key+"4",
		hrpc.Families(map[string][]string{"cf": nil}),
		hrpc.NumberOfRows(1)) // fetch only one row at a time
	if err != nil {
		t.Fatalf("Scan req failed: %s", err)
	}
	scanner := c.Scan(scan)
	res, err := scanner.Next()
	checkResultRow(t, res, key+"1", err, nil)

	res, err = scanner.Next()
	checkResultRow(t, res, key+"2", err, nil)

	scanner.Close()

	// make sure we get io.EOF eventually
	for {
		if _, err = scanner.Next(); err == io.EOF {
			break
		}
	}
}

func TestScannerContextCancel(t *testing.T) {
	key := t.Name()
	c := gohbase.NewClient(*host)
	defer c.Close()

	err := insertKeyValue(c, key+"1", "cf", []byte("1"))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key+"2", "cf", []byte("1"))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	err = insertKeyValue(c, key+"3", "cf", []byte("1"))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	scan, err := hrpc.NewScanRangeStr(ctx, table,
		key+"1", key+"4",
		hrpc.Families(map[string][]string{"cf": nil}),
		hrpc.NumberOfRows(1)) // fetch only one row at a time
	if err != nil {
		t.Fatalf("Scan req failed: %s", err)
	}
	scanner := c.Scan(scan)
	res, err := scanner.Next()
	checkResultRow(t, res, key+"1", err, nil)

	cancel()

	if _, err = scanner.Next(); err != context.Canceled {
		t.Fatalf("unexpected error %v, expected %v", err, context.Canceled)
	}
}

func TestAppend(t *testing.T) {
	key := "row7"
	c := gohbase.NewClient(*host)
	defer c.Close()
	// Inserting "Hello"
	insertErr := insertKeyValue(c, key, "cf", []byte("Hello"))
	if insertErr != nil {
		t.Errorf("Put returned an error: %v", insertErr)
	}
	// Appending " my name is Dog."
	values := map[string]map[string][]byte{"cf": map[string][]byte{}}
	values["cf"]["a"] = []byte(" my name is Dog.")
	appRequest, err := hrpc.NewAppStr(context.Background(), table, key, values)
	appRsp, err := c.Append(appRequest)
	if err != nil {
		t.Errorf("Append returned an error: %v", err)
	}
	if appRsp == nil {
		t.Errorf("Append doesn't return updated value.")
	}
	// Verifying new result is "Hello my name is Dog."
	result := appRsp.Cells[0].Value
	if !bytes.Equal([]byte("Hello my name is Dog."), result) {
		t.Errorf("Append returned an incorrect result. Expected: %v, Receieved: %v",
			[]byte("Hello my name is Dog."), result)
	}

	// Make sure the change was actually committed.
	headers := map[string][]string{"cf": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	rsp, err := c.Get(get)
	cells := rsp.Cells
	if len(cells) != 1 {
		t.Errorf("Get expected 1 cells. Received: %d", len(cells))
	}
	result = cells[0].Value
	if !bytes.Equal([]byte("Hello my name is Dog."), result) {
		t.Errorf("Append returned an incorrect result. Expected: %v, Receieved: %v",
			[]byte("Hello my name is Dog."), result)
	}
}

func TestIncrement(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()
	key := "row102"

	// test incerement
	incRequest, err := hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 1)
	result, err := c.Increment(incRequest)
	if err != nil {
		t.Fatalf("Increment returned an error: %v", err)
	}

	if result != 1 {
		t.Fatalf("Increment's result is %d, want 1", result)
	}

	incRequest, err = hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 5)
	result, err = c.Increment(incRequest)
	if err != nil {
		t.Fatalf("Increment returned an error: %v", err)
	}

	if result != 6 {
		t.Fatalf("Increment's result is %d, want 6", result)
	}
}

func TestIncrementParallel(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()
	key := "row102.5"

	numParallel := 10

	// test incerement
	var wg sync.WaitGroup
	for i := 0; i < numParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			incRequest, err := hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 1)
			_, err = c.Increment(incRequest)
			if err != nil {
				t.Errorf("Increment returned an error: %v", err)
			}
		}()
	}
	wg.Wait()

	// do one more to check if there's a correct value
	incRequest, err := hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 1)
	result, err := c.Increment(incRequest)
	if err != nil {
		t.Fatalf("Increment returned an error: %v", err)
	}

	if result != int64(numParallel+1) {
		t.Fatalf("Increment's result is %d, want %d", result, numParallel+1)
	}
}

func TestCheckAndPut(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	key := "row100"
	ef := "cf"
	eq := "a"

	var castests = []struct {
		inValues        map[string]map[string][]byte
		inExpectedValue []byte
		out             bool
	}{
		{map[string]map[string][]byte{"cf": map[string][]byte{"b": []byte("2")}},
			nil, true}, // nil instead of empty byte array
		{map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}},
			[]byte{}, true},
		{map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}},
			[]byte{}, false},
		{map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("2")}},
			[]byte("1"), true},
		{map[string]map[string][]byte{"cf": map[string][]byte{"b": []byte("2")}},
			[]byte("2"), true}, // put diff column
		{map[string]map[string][]byte{"cf": map[string][]byte{"b": []byte("2")}},
			[]byte{}, false}, // diff column
		{map[string]map[string][]byte{"cf": map[string][]byte{
			"b": []byte("100"),
			"a": []byte("100"),
		}}, []byte("2"), true}, // multiple values
	}

	for _, tt := range castests {
		putRequest, err := hrpc.NewPutStr(context.Background(), table, key, tt.inValues)
		if err != nil {
			t.Fatalf("NewPutStr returned an error: %v", err)
		}

		casRes, err := c.CheckAndPut(putRequest, ef, eq, tt.inExpectedValue)

		if err != nil {
			t.Fatalf("CheckAndPut error: %s", err)
		}

		if casRes != tt.out {
			t.Errorf("CheckAndPut with put values=%q and expectedValue=%q returned %v, want %v",
				tt.inValues, tt.inExpectedValue, casRes, tt.out)
		}
	}

	// TODO: check the resulting state by performing a Get request
}

func makeMap(cf, k, v string) map[string]map[string][]byte {
	return map[string]map[string][]byte{cf: map[string][]byte{k: []byte(v)}}
}

func TestCheckAndPutWithCompareTypeGreater(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	key := "rowCAP"
	ef := "cf"
	eq := "a"

	var testcases = []struct {
		inValues    map[string]map[string][]byte
		cmpVal      []byte
		out         bool
		expectedVal []byte
	}{
		{makeMap("cf", "a", "2"), nil, true, []byte("2")},
		{makeMap("cf", "a", "2"), nil, false, []byte("2")},
		{makeMap("cf", "b", "1"), []byte{}, false, []byte("2")},
		{makeMap("cf", "b", "1"), []byte{}, false, []byte("2")}, // Strictly greater
		{makeMap("cf", "b", "3"), []byte("1"), false, []byte("2")},
		{makeMap("cf", "a", "4"), []byte("2"), false, []byte("2")},
		{makeMap("cf", "a", "1"), []byte("99"), true, []byte("1")},
		{makeMap("cf", "b", "2"), []byte("98"), true, []byte("1")},
	}

	for _, tc := range testcases {
		putRequest, err := hrpc.NewPutStr(context.Background(), table, key, tc.inValues)
		if err != nil {
			t.Fatalf("NewPutStr returned an error: %v", err)
		}

		casRes, err := c.CheckAndPutWithCompareType(
			putRequest, ef, eq, tc.cmpVal, pb.CompareType_GREATER)

		if err != nil {
			t.Fatalf("CheckAndPut error: %s", err)
		}

		if casRes != tc.out {
			t.Errorf("CheckAndPut with put values=%q and cmpValue=%q returned %v, want %v",
				tc.inValues, tc.cmpVal, casRes, tc.out)
		}

		get, err := hrpc.NewGetStr(context.Background(), table, key,
			hrpc.Families(map[string][]string{"cf": nil}))
		rsp, err := c.Get(get)
		if err != nil {
			t.Fatalf("Get failed: %s", err)
		}
		if len(rsp.Cells) < 1 {
			t.Errorf("Get expected at least 1 cell. Received: %d", len(rsp.Cells))
		}
		if !bytes.Equal(rsp.Cells[0].Value, tc.expectedVal) {
			t.Errorf("Get expected value %q. Received: %q for inValues: %v",
				tc.expectedVal, rsp.Cells[0].Value, tc.inValues)
		}

	}

}

func TestCheckAndPutNotPut(t *testing.T) {
	key := "row101"
	c := gohbase.NewClient(*host)
	defer c.Close()
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("lol")}}

	appRequest, err := hrpc.NewAppStr(context.Background(), table, key, values)
	_, err = c.CheckAndPut(appRequest, "cf", "a", []byte{})
	if err == nil {
		t.Error("CheckAndPut: should not allow anything but Put request")
	}
}

func TestCheckAndPutParallel(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	keyPrefix := "row100.5"

	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	capTestFunc := func(p *hrpc.Mutate, ch chan bool) {
		casRes, err := c.CheckAndPut(p, "cf", "a", []byte{})

		if err != nil {
			t.Errorf("CheckAndPut error: %s", err)
		}

		ch <- casRes
	}

	// make 10 pairs of CheckAndPut requests
	for i := 0; i < 10; i++ {
		ch := make(chan bool, 2)
		putRequest1, err := hrpc.NewPutStr(
			context.Background(), table, keyPrefix+fmt.Sprint(i), values)
		if err != nil {
			t.Fatalf("NewPutStr returned an error: %v", err)
		}
		putRequest2, err := hrpc.NewPutStr(
			context.Background(), table, keyPrefix+fmt.Sprint(i), values)
		if err != nil {
			t.Fatalf("NewPutStr returned an error: %v", err)
		}

		go capTestFunc(putRequest1, ch)
		go capTestFunc(putRequest2, ch)

		first := <-ch
		second := <-ch

		if first && second {
			t.Error("CheckAndPut: both requests cannot succeed")
		}

		if !first && !second {
			t.Error("CheckAndPut: both requests cannot fail")
		}
	}
}

func TestCheckAndMutate(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	key := "row_cam_1"
	cf := "cf"
	qualifier := "a"

	// First, put an initial value
	err := insertKeyValueAtCol(c, key, qualifier, cf, []byte("100"))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		values      map[string]map[string][]byte
		compareType pb.CompareType
		compareVal  []byte
		want        bool
	}{
		{
			name:        "equal match",
			values:      map[string]map[string][]byte{cf: {qualifier: []byte("200")}},
			compareType: pb.CompareType_EQUAL,
			compareVal:  []byte("100"),
			want:        true,
		},
		{
			name:        "equal no match",
			values:      map[string]map[string][]byte{cf: {qualifier: []byte("300")}},
			compareType: pb.CompareType_EQUAL,
			compareVal:  []byte("100"),
			want:        false,
		},
		{
			name:        "not equal match",
			values:      map[string]map[string][]byte{cf: {qualifier: []byte("300")}},
			compareType: pb.CompareType_NOT_EQUAL,
			compareVal:  []byte("100"),
			want:        true,
		},
		{
			name:        "greater match",
			values:      map[string]map[string][]byte{cf: {qualifier: []byte("400")}},
			compareType: pb.CompareType_GREATER,
			// Condition: comparatorValue > currentValue ("300")
			// "400" > "300" is true, so mutation should be applied
			compareVal: []byte("400"),
			want:       true,
		},
		{
			name:        "less match not applied",
			values:      map[string]map[string][]byte{cf: {qualifier: []byte("500")}},
			compareType: pb.CompareType_LESS,
			// Condition: comparatorValue < currentValue ("400")
			// "600" < "400" is false, so mutation should not be applied
			compareVal: []byte("600"),
			want:       false,
		},
		{
			name:        "less match applied",
			values:      map[string]map[string][]byte{cf: {qualifier: []byte("500")}},
			compareType: pb.CompareType_LESS,
			// Condition: comparatorValue < currentValue ("400")
			// "300" < "400" is true, so mutation should be applied
			compareVal: []byte("300"),
			want:       true,
		},
	}

	// Note that these cases were meant to be run sequentially, not in parallel
	for _, tt := range tests {
		putReq, err := hrpc.NewPutStr(context.Background(), table, key, tt.values)
		if err != nil {
			t.Fatal(err)
		}

		cmp := filter.NewBinaryComparator(filter.NewByteArrayComparable(tt.compareVal))
		cam, err := hrpc.NewCheckAndMutate(putReq, cf, qualifier, tt.compareType, cmp)
		if err != nil {
			t.Fatal(err)
		}

		result, err := c.CheckAndMutate(cam)
		if err != nil {
			t.Fatal(err)
		}

		if result != tt.want {
			t.Errorf("got %v, want %v", result, tt.want)
		}
	}
}

func TestCheckAndMutateDelete(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	key := "row_cam_del"
	cf := "cf"
	qualifier := "a"

	tests := []struct {
		name           string
		compareType    pb.CompareType
		compareVal     []byte
		wantProcessed  bool
		wantCellsAfter int
	}{
		{
			name:           "not equal no match",
			compareType:    pb.CompareType_NOT_EQUAL,
			compareVal:     []byte("delete_me"),
			wantCellsAfter: 1,
		},
		{
			name:           "equal match deletes",
			compareType:    pb.CompareType_EQUAL,
			compareVal:     []byte("delete_me"),
			wantProcessed:  true,
			wantCellsAfter: 0,
		},
	}

	for _, tt := range tests {
		// Put initial value before each test case
		initVals := map[string]map[string][]byte{cf: {qualifier: []byte("delete_me")}}
		putReq, err := hrpc.NewPutStr(context.Background(), table, key, initVals)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if _, err = c.Put(putReq); err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		// Attempt conditional delete
		delReq, err := hrpc.NewDelStr(context.Background(), table, key, nil)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		cmp := filter.NewBinaryComparator(filter.NewByteArrayComparable(tt.compareVal))
		cam, err := hrpc.NewCheckAndMutate(delReq, cf, qualifier, tt.compareType, cmp)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		result, err := c.CheckAndMutate(cam)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if result != tt.wantProcessed {
			t.Errorf("%s: got processed=%v, want %v", tt.name, result, tt.wantProcessed)
		}

		// Verify cell count
		getReq, err := hrpc.NewGetStr(context.Background(), table, key)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		getRes, err := c.Get(getReq)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if len(getRes.Cells) != tt.wantCellsAfter {
			t.Errorf("%s: got %d cells, want %d", tt.name, len(getRes.Cells), tt.wantCellsAfter)
		}
	}
}

func TestCheckAndMutateWithFilter(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	key := "row_cam_filter"
	cf := "cf"

	tests := []struct {
		name           string
		setupData      map[string][]byte
		filter         filter.Filter
		mutationVal    []byte
		wantProcessed  bool
		wantFinalValue []byte
	}{
		{
			name: "single qualifier check - match",
			setupData: map[string][]byte{
				"key1": []byte("value1"),
			},
			filter: filter.NewSingleColumnValueFilter(
				[]byte(cf), []byte("key1"),
				filter.Equal,
				filter.NewBinaryComparator(
					filter.NewByteArrayComparable([]byte("value1"))),
				false, false),
			mutationVal:    []byte("updated"),
			wantProcessed:  true,
			wantFinalValue: []byte("updated"),
		},
		{
			name: "single qualifier check - no match",
			setupData: map[string][]byte{
				"key1": []byte("value1"),
			},
			filter: filter.NewSingleColumnValueFilter(
				[]byte(cf), []byte("key1"),
				filter.Equal,
				filter.NewBinaryComparator(
					filter.NewByteArrayComparable([]byte("wrong_value"))),
				false, false),
			mutationVal:    []byte("should_not_update"),
			wantProcessed:  false,
			wantFinalValue: []byte("value1"),
		},
		{
			name: "multiple qualifiers AND logic (MustPassAll) - all match",
			setupData: map[string][]byte{
				"key1": []byte("v1"),
				"key2": []byte("v2"),
			},
			filter: filter.NewList(filter.MustPassAll,
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key1"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("v1"))),
					false, false),
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key2"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("v2"))),
					false, false),
			),
			mutationVal:    []byte("all_matched"),
			wantProcessed:  true,
			wantFinalValue: []byte("all_matched"),
		},
		{
			name: "multiple qualifiers AND logic (MustPassAll) - one fails",
			setupData: map[string][]byte{
				"key1": []byte("v1"),
				"key2": []byte("v2"),
			},
			filter: filter.NewList(filter.MustPassAll,
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key1"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("v1"))),
					false, false),
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key2"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("wrong"))),
					false, false),
			),
			mutationVal:    []byte("should_not_update"),
			wantProcessed:  false,
			wantFinalValue: []byte("v1"),
		},
		{
			name: "multiple qualifiers OR logic (MustPassOne) - one matches",
			setupData: map[string][]byte{
				"key1": []byte("v1"),
				"key2": []byte("v2"),
			},
			filter: filter.NewList(filter.MustPassOne,
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key1"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("v1"))),
					false, false),
				filter.NewSingleColumnValueFilter(
					[]byte(cf), []byte("key2"),
					filter.Equal,
					filter.NewBinaryComparator(
						filter.NewByteArrayComparable([]byte("wrong"))),
					false, false),
			),
			mutationVal:    []byte("one_matched"),
			wantProcessed:  true,
			wantFinalValue: []byte("one_matched"),
		},
	}

	// Note that these cases were meant to be run sequentially, not in parallel
	for _, tt := range tests {
		putReq, err := hrpc.NewPutStr(context.Background(), table, key,
			map[string]map[string][]byte{cf: tt.setupData})
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if _, err = c.Put(putReq); err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		mutationReq, err := hrpc.NewPutStr(context.Background(), table, key,
			map[string]map[string][]byte{cf: {"key1": tt.mutationVal}})
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		cam, err := hrpc.NewCheckAndMutateWithFilter(mutationReq, tt.filter)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		result, err := c.CheckAndMutate(cam)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		if result != tt.wantProcessed {
			t.Errorf("%s: got processed=%v, want %v",
				tt.name, result, tt.wantProcessed)
		}

		getReq, err := hrpc.NewGetStr(context.Background(), table, key)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		getRes, err := c.Get(getReq)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		var actualValue []byte
		for _, cell := range getRes.Cells {
			if string(cell.Qualifier) == "key1" {
				actualValue = cell.Value
				break
			}
		}

		if !bytes.Equal(actualValue, tt.wantFinalValue) {
			t.Errorf("%s: got value=%q, want %q",
				tt.name, actualValue, tt.wantFinalValue)
		}
	}
}

func TestClose(t *testing.T) {
	c := gohbase.NewClient(*host)

	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	r, err := hrpc.NewPutStr(context.Background(), table, t.Name(), values)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Put(r)
	if err != nil {
		t.Fatal(err)
	}

	c.Close()

	_, err = c.Put(r)
	if err != gohbase.ErrClientClosed {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloseWithoutMeta(t *testing.T) {
	c := gohbase.NewClient(*host)
	c.Close()

	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	r, err := hrpc.NewPutStr(context.Background(), table, t.Name(), values)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Put(r)
	if err != gohbase.ErrClientClosed {
		t.Fatalf("unexpected error: %v", err)
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
	defer c.Close()
	err := insertKeyValue(c, key, "cf", val)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	// RegionServer 1 hosts all the current regions.
	// Now launch servers 2,3
	LocalRegionServersCmd(t, "start", []string{"2", "3"})
	t.Cleanup(func() {
		LocalRegionServersCmd(t, "stop", []string{"2", "3"})
	})
	ac := gohbase.NewAdminClient(*host)

	_, err = waitForRegionServers(t, ac, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Now (gracefully) stop servers 1,2.
	// All regions should now be on server 3.
	LocalRegionServersCmd(t, "stop", []string{"1", "2"})
	t.Cleanup(func() {
		LocalRegionServersCmd(t, "start", []string{"1"})
	})

	_, err = waitForRegionServers(t, ac, 1)
	if err != nil {
		t.Fatal(err)
	}

	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	rsp_value := rsp.Cells[0].Value
	if !bytes.Equal(rsp_value, val) {
		t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
			val, rsp_value)
	}
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
	defer c.Close()
	b.ResetTimer()
	headers := map[string][]string{"cf": nil}
	for i := 0; i < b.N; i++ {
		key := keyPrefix + fmt.Sprintf("%d", i)
		get, _ := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
		c.Get(get)
	}
}

// Helper function. Given a key_prefix, num_ops, performs num_ops.
func performNPuts(keyPrefix string, num_ops int) error {
	c := gohbase.NewClient(*host)
	defer c.Close()
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
func insertKeyValue(c gohbase.Client, key, columnFamily string, value []byte,
	options ...func(hrpc.Call) error) error {
	return insertKeyValueAtCol(c, key, "a", columnFamily, value, options...)
}

// insertKeyValueAtCol inserts a value into the table under column col at key.
func insertKeyValueAtCol(c gohbase.Client, key, col, columnFamily string, value []byte,
	options ...func(hrpc.Call) error) error {
	values := map[string]map[string][]byte{columnFamily: map[string][]byte{}}
	values[columnFamily][col] = value
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values, options...)
	if err != nil {
		return err
	}
	_, err = c.Put(putRequest)
	return err
}

func deleteKeyValue(c gohbase.Client, key, columnFamily string, value []byte,
	options ...func(hrpc.Call) error) error {
	values := map[string]map[string][]byte{columnFamily: map[string][]byte{}}
	values[columnFamily]["a"] = value
	d, err := hrpc.NewDel(context.Background(), []byte(table), []byte(key), values)
	if err != nil {
		return err
	}
	_, err = c.Delete(d)
	return err
}

func TestMaxResultsPerColumnFamilyGet(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()
	key := "variablecolumnrow"
	baseErr := "MaxResultsPerColumnFamilyGet error "

	values := make(map[string]map[string][]byte)
	values["cf"] = map[string][]byte{}

	// Save a row with 20 columns
	for i := 0; i < 20; i++ {
		colKey := fmt.Sprintf("%02d", i)
		values["cf"][colKey] = []byte(fmt.Sprintf("value %d", i))
	}

	// First test that the function can't be used on types other than get or scan
	putRequest, err := hrpc.NewPutStr(context.Background(),
		table,
		key,
		values,
		hrpc.MaxResultsPerColumnFamily(5),
	)
	if err == nil {
		t.Errorf(baseErr+"- Option allowed to be used with incorrect type: %s", err)
	}
	putRequest, err = hrpc.NewPutStr(context.Background(),
		table,
		key,
		values,
		hrpc.ResultOffset(5),
	)
	if err == nil {
		t.Errorf(baseErr+"- Option allowed to be used with incorrect type: %s", err)
	}

	// Now actually save the values
	putRequest, err = hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		t.Errorf(baseErr+"building put string: %s", err)

	}
	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf(baseErr+"saving row: %s", err)
	}

	family := hrpc.Families(map[string][]string{"cf": nil})
	// Do we get the correct number of cells without qualifier
	getRequest, err := hrpc.NewGetStr(context.Background(),
		table,
		key,
		family,
		hrpc.MaxVersions(1),
	)
	if err != nil {
		t.Errorf(baseErr+"building get request: %s", err)
	}
	result, err := c.Get(getRequest)
	if len(result.Cells) != 20 {
		t.Errorf(baseErr+"- expecting %d results with parameters; received %d",
			20,
			len(result.Cells),
		)
	}

	// Simple test for max columns per column family. Return the first n columns in order
	for testCnt := 1; testCnt <= 20; testCnt++ {
		// Get the first n columns
		getRequest, err := hrpc.NewGetStr(context.Background(),
			table,
			key,
			family,
			hrpc.MaxVersions(1),
			hrpc.MaxResultsPerColumnFamily(uint32(testCnt)),
		)
		if err != nil {
			t.Errorf(baseErr+"building get request: %s", err)
		}
		result, err := c.Get(getRequest)
		if len(result.Cells) != testCnt {
			t.Errorf(baseErr+"- expecting %d results; received %d", testCnt, len(result.Cells))
		}
		for i, x := range result.Cells {
			// Make sure the column name and value are what is expected and in correct sequence
			if string(x.Qualifier) != fmt.Sprintf("%02d", i) ||
				string(x.Value) != fmt.Sprintf("value %d", i) {
				t.Errorf(baseErr+"- unexpected return value. Expecting %s received %s",
					fmt.Sprintf("value %d", i),
					string(x.Value),
				)
			}
		}

		// Get with out of range values
		getRequest, err = hrpc.NewGetStr(context.Background(),
			table,
			key,
			family,
			hrpc.MaxVersions(1),
			hrpc.MaxResultsPerColumnFamily(math.MaxUint32),
		)
		if err == nil {
			t.Error(baseErr + "- out of range column result parameter accepted")
		}
		// Get with out of range values
		getRequest, err = hrpc.NewGetStr(context.Background(),
			table,
			key,
			family,
			hrpc.MaxVersions(1),
			hrpc.ResultOffset(math.MaxUint32),
		)
		if err == nil {
			t.Error(baseErr + "- out of range column offset parameter accepted")
		}

	}

	// Max columns per column family. Return first n cells in order with offset.
	for offset := 0; offset < 20; offset++ {
		for maxResults := 1; maxResults <= 20-offset; maxResults++ {
			getRequest, err := hrpc.NewGetStr(context.Background(),
				table,
				key,
				family,
				hrpc.MaxVersions(1),
				hrpc.MaxResultsPerColumnFamily(uint32(maxResults)),
				hrpc.ResultOffset(uint32(offset)),
			)
			if err != nil {
				t.Errorf(baseErr+"building get request testing offset: %s", err)
			}
			result, err := c.Get(getRequest)

			// Make sure number of cells returned is still correct
			if len(result.Cells) != maxResults {
				t.Errorf(baseErr+"with offset - expecting %d results; received %d",
					maxResults,
					len(result.Cells),
				)
			}
			// make sure the cells returned are what is expected and in correct sequence
			for i, _ := range result.Cells {
				if string(result.Cells[i].Value) != fmt.Sprintf("value %d", offset+i) {
					t.Errorf(baseErr+"with offset - Expected value %s but received %s",
						fmt.Sprintf("value %d", offset+i),
						string(result.Cells[i].Value),
					)
				}
			}
		}
	}
}

func TestMaxResultsPerColumnFamilyScan(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	baseErr := "MaxResultsPerColumnFamilyScan error "
	key := "variablecolumnrow_1"

	values := make(map[string]map[string][]byte)
	values["cf"] = map[string][]byte{}

	// Save a row with 20 columns
	for i := 0; i < 20; i++ {
		colKey := fmt.Sprintf("%02d", i)
		values["cf"][colKey] = []byte(fmt.Sprintf("value %d", i))
	}
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		t.Errorf(baseErr+"building put string: %s", err)

	}
	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf(baseErr+"saving row: %s", err)
	}
	// Save another row with 20 columns
	key = "variablecolumnrow_2"
	putRequest, err = hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		t.Errorf(baseErr+"building put string: %s", err)

	}
	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf(baseErr+"saving row: %s", err)
	}

	family := hrpc.Families(map[string][]string{"cf": nil})
	pFilter := filter.NewPrefixFilter([]byte("variablecolumnrow_"))

	// Do we get the correct number of cells without qualifier
	scanRequest, err := hrpc.NewScanStr(context.Background(),
		table,
		family,
		hrpc.Filters(pFilter),
		hrpc.MaxVersions(1),
	)
	if err != nil {
		t.Errorf(baseErr+"building scan request: %s", err)
	}

	result := c.Scan(scanRequest)
	resultCnt := 0
	for {
		rRow, err := result.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf(baseErr+"scanning result: %s", err)
		}
		if len(rRow.Cells) != 20 {
			t.Errorf(baseErr+"- expected all 20 columns but received %d", len(rRow.Cells))
		}
		resultCnt++
	}
	if resultCnt != 2 {
		t.Errorf(baseErr+"- expected 2 rows; received %d", resultCnt)
	}

	// Do we get a limited number of columns per row
	baseErr = "MaxResultsPerColumnFamilyScan with limited columns error "
	scanRequest, err = hrpc.NewScanStr(context.Background(),
		table,
		family,
		hrpc.Filters(pFilter),
		hrpc.MaxVersions(1),
		hrpc.MaxResultsPerColumnFamily(15),
	)
	if err != nil {
		t.Errorf(baseErr+"building scan request: %s", err)
	}

	result = c.Scan(scanRequest)
	resultCnt = 0
	for {
		rRow, err := result.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf(baseErr+"scanning result: %s", err)
		}
		if len(rRow.Cells) != 15 {
			t.Errorf(baseErr+"- expected 15 columns but received %d", len(rRow.Cells))
		}
		resultCnt++
	}
	if resultCnt != 2 {
		t.Errorf(baseErr+"- expected 2 rows; received %d", resultCnt)
	}

	// Do we get a limited number of columns per row and are they correctly offset
	baseErr = "MaxResultsPerColumnFamilyScan with limited columns and offset error "
	scanRequest, err = hrpc.NewScanStr(context.Background(),
		table,
		family,
		hrpc.Filters(pFilter),
		hrpc.MaxVersions(1),
		hrpc.MaxResultSize(1),
		hrpc.MaxResultsPerColumnFamily(2),
		hrpc.ResultOffset(10),
	)
	if err != nil {
		t.Errorf(baseErr+"building scan request: %s", err)
	}

	result = c.Scan(scanRequest)
	resultCnt = 0
	for {
		rRow, err := result.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf(baseErr+"scanning result: %s", err)
		}
		if len(rRow.Cells) != 2 {
			t.Errorf(baseErr+"- expected 2 columns but received %d", len(rRow.Cells))
		}
		if string(rRow.Cells[0].Value) != "value 10" || string(rRow.Cells[1].Value) != "value 11" {
			t.Errorf(baseErr+"- unexpected cells values. "+
				"Expected 'value 10' and 'value 11' - received %s and %s",
				string(rRow.Cells[0].Value),
				string(rRow.Cells[1].Value),
			)
		}
		resultCnt++
	}
	if resultCnt != 2 {
		t.Errorf(baseErr+"- expected 1 row; received %d", resultCnt)
	}

	// Test with out of range values
	scanRequest, err = hrpc.NewScanStr(context.Background(),
		table,
		family,
		hrpc.Filters(pFilter),
		hrpc.MaxVersions(1),
		hrpc.MaxResultsPerColumnFamily(math.MaxUint32),
	)
	if err == nil {
		t.Error(baseErr + "- out of range column result parameter accepted")
	}
	scanRequest, err = hrpc.NewScanStr(context.Background(),
		table,
		family,
		hrpc.Filters(pFilter),
		hrpc.MaxVersions(1),
		hrpc.ResultOffset(math.MaxUint32),
	)
	if err == nil {
		t.Error(baseErr + "- out of range column result parameter accepted")
	}

}

func TestMultiRequest(t *testing.T) {
	// pre-populate the table
	var (
		getKey       = t.Name() + "_Get"
		putKey       = t.Name() + "_Put"
		deleteKey    = t.Name() + "_Delete"
		appendKey    = t.Name() + "_Append"
		incrementKey = t.Name() + "_Increment"
	)
	c := gohbase.NewClient(*host, gohbase.RpcQueueSize(1))
	if err := insertKeyValue(c, getKey, "cf", []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := insertKeyValue(c, deleteKey, "cf", []byte{3}); err != nil {
		t.Fatal(err)
	}
	if err := insertKeyValue(c, appendKey, "cf", []byte{4}); err != nil {
		t.Fatal(err)
	}
	i, err := hrpc.NewIncStrSingle(context.Background(), table, incrementKey, "cf", "a", 5)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Increment(i)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	c = gohbase.NewClient(*host, gohbase.FlushInterval(1000*time.Hour), gohbase.RpcQueueSize(5))
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		g, err := hrpc.NewGetStr(context.Background(), table, getKey)
		if err != nil {
			t.Error(err)
		}
		r, err := c.Get(g)
		if err != nil {
			t.Error(err)
		}
		expV := []byte{1}
		if !bytes.Equal(r.Cells[0].Value, expV) {
			t.Errorf("expected %v, got %v:", expV, r.Cells[0].Value)
		}
		wg.Done()
	}()

	go func() {
		v := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte{2}}}
		p, err := hrpc.NewPutStr(context.Background(), table, putKey, v)
		if err != nil {
			t.Error(err)
		}
		r, err := c.Put(p)
		if err != nil {
			t.Error(err)
		}
		if len(r.Cells) != 0 {
			t.Errorf("expected no cells, got %d", len(r.Cells))
		}
		wg.Done()
	}()

	go func() {
		d, err := hrpc.NewDelStr(context.Background(), table, deleteKey, nil)
		if err != nil {
			t.Error(err)
		}
		r, err := c.Delete(d)
		if err != nil {
			t.Error(err)
		}
		if len(r.Cells) != 0 {
			t.Errorf("expected no cells, got %d", len(r.Cells))
		}
		wg.Done()
	}()

	go func() {
		v := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte{4}}}
		a, err := hrpc.NewAppStr(context.Background(), table, appendKey, v)
		if err != nil {
			t.Error(err)
		}
		r, err := c.Append(a)
		if err != nil {
			t.Error(err)
		}
		expV := []byte{4, 4}
		if !bytes.Equal(r.Cells[0].Value, expV) {
			t.Errorf("expected %v, got %v:", expV, r.Cells[0].Value)
		}
		wg.Done()
	}()

	go func() {
		i, err := hrpc.NewIncStrSingle(context.Background(), table, incrementKey, "cf", "a", 1)
		if err != nil {
			t.Error(err)
		}
		r, err := c.Increment(i)
		if err != nil {
			t.Error(err)
		}
		if r != 6 {
			t.Errorf("expected %d, got %d:", 6, r)
		}
		wg.Done()
	}()

	wg.Wait()
}

func TestReverseScan(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	const baseErr = "Reverse Scan error "

	values := make(map[string]map[string][]byte)
	values["cf"] = map[string][]byte{}

	// Save 500 rows
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("REVTEST-%03d", i)
		values["cf"]["reversetest"] = []byte(fmt.Sprintf("%d", i))
		putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
		if err != nil {
			t.Errorf(baseErr+"building put string: %s", err)

		}

		_, err = c.Put(putRequest)
		if err != nil {
			t.Errorf(baseErr+"saving row: %s", err)
		}
	}

	// Read them back in reverse order
	scanRequest, err := hrpc.NewScanRangeStr(context.Background(),
		table,
		"REVTEST-999",
		"REVTEST-",
		hrpc.Families(map[string][]string{"cf": []string{"reversetest"}}),
		hrpc.Reversed(),
	)
	if err != nil {
		t.Errorf(baseErr+"setting up reverse scan: %s", err)
	}
	i := 0
	results := c.Scan(scanRequest)
	for {
		r, err := results.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Errorf(baseErr+"scanning results: %s", err)
		}
		i++
		expected := fmt.Sprintf("%d", 500-i)
		if string(r.Cells[0].Value) != expected {
			t.Error(baseErr + "- unexpected rowkey returned")
		}
	}
	if i != 500 {
		t.Errorf(baseErr+" expected 500 rows returned; found %d", i)
	}
	results.Close()

	// Read part of them back in reverse order. Stoprow should be exclusive just like forward scan
	scanRequest, err = hrpc.NewScanRangeStr(context.Background(),
		table,
		"REVTEST-250",
		"REVTEST-150",
		hrpc.Families(map[string][]string{"cf": []string{"reversetest"}}),
		hrpc.Reversed(),
	)
	if err != nil {
		t.Errorf(baseErr+"setting up reverse scan: %s", err)
	}
	i = 0
	results = c.Scan(scanRequest)
	for {
		r, err := results.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Errorf(baseErr+"scanning results: %s", err)
		}
		i++
		expected := fmt.Sprintf("%d", 251-i)
		if string(r.Cells[0].Value) != expected {
			t.Error(baseErr + "- unexpected rowkey returned when doing partial reverse scan")
		}
	}
	if i != 100 {
		t.Errorf(baseErr+" expected 100 rows returned; found %d", i)
	}
	results.Close()

}

func TestListTableNames(t *testing.T) {
	// Initialize our tables
	ac := gohbase.NewAdminClient(*host)
	tables := []string{
		table + "_MATCH1",
		table + "_MATCH2",
		table + "nomatch",
	}

	for _, tn := range tables {
		// Since this test is called by TestMain which waits for hbase init
		// there is no need to wait here.
		err := CreateTable(ac, tn, []string{"cf"})
		if err != nil {
			panic(err)
		}
	}

	defer func() {
		for _, tn := range tables {
			err := DeleteTable(ac, tn)
			if err != nil {
				panic(err)
			}
		}
	}()

	m1 := []byte(table + "_MATCH1")
	m2 := []byte(table + "_MATCH2")
	tcases := []struct {
		desc      string
		regex     string
		namespace string
		sys       bool

		match []*pb.TableName
	}{
		{
			desc:  "match all",
			regex: ".*",
			match: []*pb.TableName{
				&pb.TableName{Qualifier: []byte(table)},
				&pb.TableName{Qualifier: m1},
				&pb.TableName{Qualifier: m2},
				&pb.TableName{Qualifier: []byte(table + "nomatch")},
			},
		},
		{
			desc:  "match_some",
			regex: ".*_MATCH.*",
			match: []*pb.TableName{
				&pb.TableName{Qualifier: m1},
				&pb.TableName{Qualifier: m2},
			},
		},
		{
			desc: "match_none",
		},
		{
			desc:      "match meta",
			regex:     ".*meta.*",
			namespace: "hbase",
			sys:       true,
			match: []*pb.TableName{
				&pb.TableName{Qualifier: []byte("meta")},
			},
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.desc, func(t *testing.T) {
			tn, err := hrpc.NewListTableNames(
				context.Background(),
				hrpc.ListRegex(tcase.regex),
				hrpc.ListSysTables(tcase.sys),
				hrpc.ListNamespace(tcase.namespace),
			)
			if err != nil {
				t.Fatal(err)
			}

			names, err := ac.ListTableNames(tn)
			if err != nil {
				t.Error(err)
			}

			// filter to have only tables that we've created
			var got []*pb.TableName
			for _, m := range names {
				if strings.HasPrefix(string(m.Qualifier), table) ||
					string(m.Namespace) == "hbase" {
					got = append(got, m)
				}
			}

			if len(got) != len(tcase.match) {
				t.Errorf("expected %v, got %v", tcase.match, got)
			}

			for i, m := range tcase.match {
				want := string(m.Qualifier)
				got := string(tcase.match[i].Qualifier)
				if want != got {
					t.Errorf("index %d: expected: %v, got %v", i, want, got)
				}
			}
		})
	}

}

// Test snapshot creation
func TestSnapshot(t *testing.T) {
	ac := gohbase.NewAdminClient(*host)

	name := "snapshot-" + table

	sn, err := hrpc.NewSnapshot(context.Background(), name, table)
	if err != nil {
		t.Fatal(err)
	}
	if err = ac.CreateSnapshot(sn); err != nil {
		t.Error(err)
	}

	defer func() {
		if err = ac.DeleteSnapshot(sn); err != nil {
			t.Error(err)
		}
	}()

	ls := hrpc.NewListSnapshots(context.Background())
	snaps, err := ac.ListSnapshots(ls)
	if err != nil {
		t.Error(err)
	}

	if len(snaps) != 1 {
		t.Errorf("expection 1 snapshot, got %v", len(snaps))
	}

	gotName := snaps[0].GetName()
	if gotName != name {
		t.Errorf("expection snapshot name to be %v got %v", name, gotName)
	}
}

// TestRestoreSnapshot tests using a snapshot to restore a table.
func TestRestoreSnapshot(t *testing.T) {
	// Procedure for this test is roughly:
	// - Create some data in a table.
	// - Create a snapshot.
	// - Remove all data.
	// - Restore snapshot.
	// - Ensure data is there.

	var (
		key  = t.Name() + "_Get"
		name = "snapshot-" + table
	)

	c := gohbase.NewClient(*host, gohbase.RpcQueueSize(1))
	if err := insertKeyValue(c, key, "cf", []byte{1}); err != nil {
		t.Fatal(err)
	}

	ac := gohbase.NewAdminClient(*host)

	sn, err := hrpc.NewSnapshot(context.Background(), name, table)
	if err != nil {
		t.Fatal(err)
	}
	if err := ac.CreateSnapshot(sn); err != nil {
		t.Error(err)
	}

	defer func() {
		err = ac.DeleteSnapshot(sn)
		if err != nil {
			t.Error(err)
		}
	}()

	if err := deleteKeyValue(c, key, "cf", []byte{1}); err != nil {
		t.Error(err)
	}

	g, err := hrpc.NewGetStr(context.Background(), table, key)
	if err != nil {
		t.Error(err)
	}

	r, err := c.Get(g)
	if err != nil {
		t.Error(err)
	}

	if len(r.Cells) != 0 {
		t.Fatalf("expected no cells in table %s key %s", table, key)
	}

	c.Close()

	td := hrpc.NewDisableTable(context.Background(), []byte(table))
	if err := ac.DisableTable(td); err != nil {
		t.Error(err)
	}

	if err = ac.RestoreSnapshot(sn); err != nil {
		t.Error(err)
	}

	te := hrpc.NewEnableTable(context.Background(), []byte(table))
	if err := ac.EnableTable(te); err != nil {
		t.Error(err)
	}

	c = gohbase.NewClient(*host, gohbase.RpcQueueSize(1))

	r, err = c.Get(g)
	if err != nil {
		t.Error(err)
	}

	expV := []byte{1}
	if !bytes.Equal(r.Cells[0].Value, expV) {
		t.Errorf("expected %v, got %v:", expV, r.Cells[0].Value)
	}
}

func TestSetBalancer(t *testing.T) {
	ac := gohbase.NewAdminClient(*host)

	sb, err := hrpc.NewSetBalancer(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	prevState, err := ac.SetBalancer(sb)
	if err != nil {
		t.Fatal(err)
	}
	if !prevState {
		t.Fatal("expected balancer to be previously enabled")
	}

	sb, err = hrpc.NewSetBalancer(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	prevState, err = ac.SetBalancer(sb)
	if err != nil {
		t.Fatal(err)
	}
	if prevState {
		t.Fatal("expected balancer to be previously disabled")
	}
}

// getRegionNames scans meta to get the region names
func getRegionNames(t *testing.T, c gohbase.Client) []*hrpc.Result {
	scan, err := hrpc.NewScan(context.Background(),
		[]byte("hbase:meta"),
		hrpc.Families(map[string][]string{"info": []string{"regioninfo"}}))
	if err != nil {
		t.Fatal(err)
	}

	var rsp []*hrpc.Result
	scanner := c.Scan(scan)
	for {
		var res *hrpc.Result
		res, err = scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rsp = append(rsp, res)
	}

	return rsp
}

func getFirstRegionName(t *testing.T, c gohbase.Client) []byte {
	rsp := getRegionNames(t, c)

	if len(rsp) == 0 {
		t.Fatal("got 0 results")
	}

	// use the first region
	if len(rsp[0].Cells) == 0 {
		t.Fatal("got 0 cells")
	}

	regionName := rsp[0].Cells[0].Row
	regionName = regionName[len(regionName)-33 : len(regionName)-1]
	return regionName
}

func TestMoveRegion(t *testing.T) {
	c := gohbase.NewClient(*host)
	ac := gohbase.NewAdminClient(*host)

	regionName := getFirstRegionName(t, c)
	mr, err := hrpc.NewMoveRegion(context.Background(), regionName)
	if err != nil {
		t.Fatal(err)
	}

	if err = ac.MoveRegion(mr); err != nil {
		t.Fatal(err)
	}
}

func TestDebugState(t *testing.T) {
	key := "row1"
	val := []byte("1")
	if host == nil {
		t.Fatal("Host is not set!")
	}

	c := gohbase.NewClient(*host)
	defer c.Close()
	err := insertKeyValue(c, key, "cf", val)
	if err != nil {
		t.Fatalf("Put returned an error: %v", err)
	}

	jsonVal, err := gohbase.DebugState(c)

	if err != nil {
		t.Fatalf("DebugState returned an error when it shouldn't have: %v", err)
	}

	var jsonUnMarshal map[string]interface{}
	err = json.Unmarshal(jsonVal, &jsonUnMarshal)

	if err != nil {
		t.Fatalf("Encoutered eror when Unmarshalling: %v", err)
	}

	clientRegionMap := jsonUnMarshal["ClientRegionMap"]
	clientType := jsonUnMarshal["ClientType"]
	regionInfoMap := jsonUnMarshal["RegionInfoMap"]
	keyRegionCache := jsonUnMarshal["KeyRegionCache"]
	clientRegionCache := jsonUnMarshal["ClientRegionCache"]

	expectedClientRegionSize := 1
	regionInfoMapSize := 2

	assert.Equal(t, string(region.RegionClient), clientType.(string))
	assert.Equal(t, expectedClientRegionSize, len(clientRegionMap.(map[string]interface{})))
	assert.Equal(t, regionInfoMapSize, len(regionInfoMap.(map[string]interface{})))
	assert.Equal(t, 1, len(keyRegionCache.(map[string]interface{})))
	assert.Equal(t, 1, len(clientRegionCache.(map[string]interface{}))) // only have one client
}

type regionInfoAndAddr struct {
	regionInfo hrpc.RegionInfo
	addr       string
}

// Test loading region cache
func TestCacheRegions(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	// make sure region cache is empty at startup
	var jsonUnMarshalStart map[string]interface{}
	jsonVal, err := gohbase.DebugState(c)
	err = json.Unmarshal(jsonVal, &jsonUnMarshalStart)
	if err != nil {
		t.Fatalf("Encoutered eror when Unmarshalling: %v", err)
	}
	cacheLength := len(jsonUnMarshalStart["KeyRegionCache"].(map[string]interface{}))
	if cacheLength != 0 {
		t.Fatal("expected empty region cache when creating a new client")
	}

	c.CacheRegions([]byte(table))

	var jsonUnMarshalCached map[string]interface{}
	jsonVal, err = gohbase.DebugState(c)
	err = json.Unmarshal(jsonVal, &jsonUnMarshalCached)
	if err != nil {
		t.Fatalf("Encoutered eror when Unmarshalling: %v", err)
	}
	// CreateTable init function starts hbase with 4 regions
	cacheLength = len(jsonUnMarshalCached["KeyRegionCache"].(map[string]interface{}))
	if cacheLength != 4 {
		t.Fatalf("Expect 4 regions but got: %v", cacheLength)
	}

}

// TestNewTableFromSnapshot tests the ability to create a snapshot from a table,
// and then use this snapshot to create a new, different table from the table the
// snapshot was created from. This is different from restoring the snapshot to the
// table it was created from.
func TestNewTableFromSnapshot(t *testing.T) {
	var (
		key          = t.Name() + "_Get"
		snapshotName = "snapshot-" + table
	)

	c := gohbase.NewClient(*host, gohbase.RpcQueueSize(1))
	defer c.Close()
	// Insert some data into the main test table.
	if err := insertKeyValue(c, key, "cf", []byte{1}); err != nil {
		t.Fatal(err)
	}

	ac := gohbase.NewAdminClient(*host)
	// Create snapshot from the main test table.
	sn, err := hrpc.NewSnapshot(context.Background(), snapshotName, table)
	if err != nil {
		t.Fatal(err)
	}
	if err = ac.CreateSnapshot(sn); err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = ac.DeleteSnapshot(sn)
		if err != nil {
			t.Error(err)
		}
	}()

	// Delete the data from main test table after taking snapshot.
	if err = deleteKeyValue(c, key, "cf", []byte{1}); err != nil {
		t.Fatal(err)
	}
	// Confirm data has been deleted.
	gMain, err := hrpc.NewGetStr(context.Background(), table, key)
	if err != nil {
		t.Fatal(err)
	}

	r, err := c.Get(gMain)
	if err != nil {
		t.Fatal(err)
	}

	if len(r.Cells) != 0 {
		t.Fatalf("expected no cells in table %s key %s", table, key)
	}

	// Restore the snapshot of the same name to a new table.
	// The new table doesn't exist yet, HBase will create it when trying to restore a snapshot to a
	// table that does not already exist. If the snapshot table doesn't exist, as in this case,
	// HBase will clone the snapshot to a new table.
	tableNew := fmt.Sprintf("gohbase_test_%d_%s", time.Now().UnixNano(), t.Name())
	sn, err = hrpc.NewSnapshot(context.Background(), snapshotName, tableNew)
	if err != nil {
		t.Fatal(err)
	}
	if err = ac.RestoreSnapshot(sn); err != nil {
		t.Fatal(err)
	}

	// It may take some time for the new table with the restored data to be created,
	// wait some time for this to complete.
	var tn *hrpc.ListTableNames
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	tn, err = hrpc.NewListTableNames(ctx, hrpc.ListRegex(tableNew))
	for {
		var names []*pb.TableName
		names, err = ac.ListTableNames(tn)
		if err != nil {
			t.Fatal(err)
		}
		if len(names) != 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	cancel()

	// Now that this table has been created, clean up after test.
	defer func() {
		err = DeleteTable(ac, tableNew)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Check that the snapshot data has been cloned to the new table.
	gNew, err := hrpc.NewGetStr(context.Background(), tableNew, key)
	if err != nil {
		t.Fatal(err)
	}
	r, err = c.Get(gNew)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Cells) == 0 {
		t.Fatal("Expected non-empty result")
	}
	expV := []byte{1}
	if !bytes.Equal(r.Cells[0].Value, expV) {
		t.Fatalf("expected %v, got %v:", expV, r.Cells[0].Value)
	}

	// Checking that the data did not get restored to the main test table:
	r, err = c.Get(gMain)
	if err != nil {
		t.Fatal(err)
	}

	if len(r.Cells) != 0 {
		t.Fatalf("expected no cells after RestoreSnapshot in table %s key %s", table, key)
	}
}

// TestScannerTimeout makes sure that without the Renew flag on we get
// a lease timeout between Next calls if the wait between them is too long.
func TestScannerTimeout(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()
	// Insert test data
	keyPrefix := "scanner_timeout_test_"
	numRows := 2
	for i := 0; i < numRows; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		value := []byte(strconv.Itoa(i))
		err := insertKeyValue(c, key, "cf", value)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Create a scan request
	// renewal is default set to false
	// We set result size to 1 to force a lease timeout between Next calls
	scan, err := hrpc.NewScanStr(context.Background(), table,
		hrpc.Families(map[string][]string{"cf": nil}),
		hrpc.Filters(filter.NewPrefixFilter([]byte(keyPrefix))),
		hrpc.NumberOfRows(1),
	)
	if err != nil {
		t.Fatalf("Failed to create scan request: %v", err)
	}

	scanner := c.Scan(scan)
	defer scanner.Close()
	rsp, err := scanner.Next()
	if err != nil {
		t.Fatalf("Scanner.Next() returned error: %v", err)
	}
	if rsp == nil {
		t.Fatalf("Unexpected end of scanner")
	}
	expectedValue := []byte(strconv.Itoa(0))
	if !bytes.Equal(rsp.Cells[0].Value, expectedValue) {
		t.Errorf("Unexpected value. Got %v, want %v", rsp.Cells[0].Value, expectedValue)
	}

	// force lease timeout
	time.Sleep(scannerLease * 2)

	_, err = scanner.Next()

	// lease timeout should return an UnknownScannerException
	if err != nil && strings.Contains(err.Error(),
		"org.apache.hadoop.hbase.UnknownScannerException") {
		t.Log("Error matches: UnknownScannerException")
	} else {
		t.Fatalf("Error does not match org.apache.hadoop.hbase.UnknownScannerException, "+
			"got: %v", err)
	}
}

// getEncodedRegionServerNames returns a slice of string encoded regionserver names of all live
// regionservers in the cluster
func getEncodedRegionServerNames(t *testing.T, ac gohbase.AdminClient) []string {
	cs, err := ac.ClusterStatus()
	if err != nil {
		t.Fatalf("Failed to get cluster status: %v", err)
	}

	var serverNames []string
	for _, server := range cs.LiveServers {
		en := fmt.Sprintf("%s,%d,%d",
			server.GetServer().GetHostName(),
			server.GetServer().GetPort(),
			server.GetServer().GetStartCode())
		serverNames = append(serverNames, en)
	}

	return serverNames
}

// getRsForRegion returns the encoded regionserver name that is hosting the given region.
// Empty if the region isn't found on any of the current live regionservers.
func getRsForRegion(t *testing.T, ac gohbase.AdminClient, regionName []byte) string {
	cs, err := ac.ClusterStatus()
	if err != nil {
		t.Fatalf("Failed to get cluster status: %v", err)
	}

	for _, server := range cs.LiveServers {
		en := fmt.Sprintf("%s,%d,%d",
			server.GetServer().GetHostName(),
			server.GetServer().GetPort(),
			server.GetServer().GetStartCode())
		for _, rl := range server.GetServerLoad().GetRegionLoads() {
			if encodedRegionNameFromRegionSpecifier(rl.GetRegionSpecifier()) == string(regionName) {
				t.Logf("Found region %s on server %s", regionName, en)
				return en
			}
		}
	}
	return ""
}

// encodedRegionNameFromRegionSpecifier returns the encoded region name from a RegionSpecifier
func encodedRegionNameFromRegionSpecifier(rs *pb.RegionSpecifier) string {
	if rs == nil {
		return ""
	}

	full := string(rs.GetValue())

	switch rs.GetType() {
	case pb.RegionSpecifier_ENCODED_REGION_NAME:
		return full
	case pb.RegionSpecifier_REGION_NAME:
		full = strings.TrimSuffix(full, ".")
		i := strings.LastIndex(full, ".")
		if i >= 0 && i+1 < len(full) {
			return full[i+1:]
		}
		return full
	default:
		return ""
	}
}

// waitForRegionServers polls the regionservers in the clusters and
// waits for the requested number of regionservers to be up
func waitForRegionServers(t *testing.T, ac gohbase.AdminClient,
	numRS int) ([]string, error) {
	timeout := time.Minute * time.Duration(numRS)
	start := time.Now()
	var serverNames []string
	var err error
	for {
		serverNames = getEncodedRegionServerNames(t, ac)
		t.Logf("Live regionserver names: %v", serverNames)
		if len(serverNames) == numRS {
			t.Logf("%d regionserver(s) up, wait 10 seconds to initalize", numRS)
			time.Sleep(time.Second * 10)
			break
		} else if time.Since(start) > timeout {
			err = fmt.Errorf("Timeout waiting for region server count to reach %d,"+
				" current count: %d", numRS, len(serverNames))
			break
		}
		if len(serverNames) > numRS {
			t.Logf("Waiting 5 seconds for %d region server(s) to be removed...",
				len(serverNames)-numRS)
		} else {
			t.Logf("Waiting 5 seconds for %d region server(s) to come up...",
				numRS-len(serverNames))
		}
		time.Sleep(time.Second * 5)
	}
	return serverNames, err
}

// TestScannerRenewal tests for the renewal process of scanners
// if the renew flag is enabled for a scan request. If there is a long
// period of waiting between Next calls, the latter Next call should
// still succeed because we are renewing every lease timeout / 2 seconds
// The test uses multiple regionservers to validate the scanner renewal request is sent to the
// correct regionserver when in a multinode set up.
func TestScannerRenewal(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()
	ctx := context.Background()
	ac := gohbase.NewAdminClient(*host)

	// Start another regionserver
	LocalRegionServersCmd(t, "start", []string{"2"})
	t.Cleanup(func() {
		// Must cleanup regionserver to return to default single regionserver state.
		LocalRegionServersCmd(t, "stop", []string{"2"})
	})
	serverNames, err := waitForRegionServers(t, ac, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Loadbalancing is disabled on this cluster. So when a new regionserver is started,
	// the regions will not automatically balance to the new regionserver.
	// For this test scenario, let's move only the region the scan goes to to the new regionserver.
	// Test uses data from 4th region (See keySplits)
	keyPrefix := "scanner_renewal_test_"
	rns := getRegionNames(t, c)

	if len(rns) == 0 {
		t.Fatal("got 0 results")
	}

	ri := 3
	if len(rns[ri].Cells) == 0 {
		t.Fatal("got 0 cells in meta for expected 4th region")
	}

	rn := rns[ri].Cells[0].Row
	rn = rn[len(rn)-33 : len(rn)-1]
	t.Log("Moving region", string(rn))

	oldRs := getRsForRegion(t, ac, rn)
	t.Logf("Region %s is starting on server %s", rn, oldRs)
	newRs := ""
	for _, sn := range serverNames {
		if sn != oldRs {
			newRs = sn
			break
		}
	}

	t.Logf("Moving region %s to server %s", rn, newRs)
	mr, err := hrpc.NewMoveRegion(ctx, rn, hrpc.WithDestinationRegionServer(newRs))
	if err != nil {
		t.Fatal(err)
	}
	if err = ac.MoveRegion(mr); err != nil {
		t.Fatal(err)
	}

	// Wait for region move to complete before proceeding:
	start := time.Now()
	timeout := time.Minute * 1
	for {
		currRs := getRsForRegion(t, ac, rn)
		t.Logf("Region %s is currently on server %s", rn, currRs)
		// When region is in transit, there will be a time during which it will not report being
		// on any server
		if currRs != "" && currRs != oldRs {
			break
		} else if time.Since(start) > timeout {
			t.Fatalf("Timeout waiting for region move to complete")
		}
		t.Log("Waiting 5 seconds for region move to complete...")
		time.Sleep(time.Second * 5)
	}

	// Insert test data
	numRows := 2
	for i := 0; i < numRows; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		value := []byte(strconv.Itoa(i))
		err := insertKeyValue(c, key, "cf", value)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Create a scan request
	// Turn on renewal
	// We set result size to 1 to force a lease timeout between Next calls
	scan, err := hrpc.NewScanStr(ctx, table,
		hrpc.Families(map[string][]string{"cf": nil}),
		hrpc.Filters(filter.NewPrefixFilter([]byte(keyPrefix))),
		hrpc.NumberOfRows(1),
		hrpc.RenewInterval(scannerLease/2),
	)
	if err != nil {
		t.Fatalf("Failed to create scan request: %v", err)
	}

	scanner := c.Scan(scan)
	defer scanner.Close()
	for i := 0; i < numRows; i++ {
		var rsp *hrpc.Result
		rsp, err = scanner.Next()
		if err != nil {
			t.Fatalf("Scanner.Next() returned error: %v", err)
		}
		if rsp == nil {
			t.Fatalf("Unexpected end of scanner")
		}
		expectedValue := []byte(strconv.Itoa(i))
		if !bytes.Equal(rsp.Cells[0].Value, expectedValue) {
			t.Fatalf("Unexpected value. Got %v, want %v", rsp.Cells[0].Value, expectedValue)
		}
		// Sleep to trigger renewal
		time.Sleep(scannerLease * 2)
	}
	// Ensure scanner is exhausted
	rsp, err := scanner.Next()
	if err != io.EOF {
		t.Fatalf("Expected EOF error, got: %v", err)
	}
	if rsp != nil {
		t.Fatalf("Expected nil response at end of scan, got: %v", rsp)
	}
}

func TestScannerRenewalCancellation(t *testing.T) {
	c := gohbase.NewClient(*host)
	defer c.Close()

	// Insert test data
	keyPrefix := "scanner_renewal_cancel_test_"
	numRows := 2
	for i := 0; i < numRows; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		value := []byte(strconv.Itoa(i))
		err := insertKeyValue(c, key, "cf", value)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scan, err := hrpc.NewScanStr(ctx, table,
		hrpc.Families(map[string][]string{"cf": nil}),
		hrpc.Filters(filter.NewPrefixFilter([]byte(keyPrefix))),
		hrpc.NumberOfRows(1),
		hrpc.RenewInterval(scannerLease/2),
	)
	if err != nil {
		t.Fatalf("Failed to create scan request: %v", err)
	}

	scanner := c.Scan(scan)
	defer scanner.Close()

	rsp, err := scanner.Next()
	if err != nil {
		t.Fatalf("Scanner.Next() returned error: %v", err)
	}
	if rsp == nil {
		t.Fatalf("Unexpected end of scanner")
	}
	expectedValue := []byte(strconv.Itoa(0))
	if !bytes.Equal(rsp.Cells[0].Value, expectedValue) {
		t.Errorf("Unexpected value. Got %v, want %v", rsp.Cells[0].Value, expectedValue)
	}

	// Cancel the context
	cancel()

	// Next call should return an error
	_, err = scanner.Next()

	if err == nil {
		t.Fatal("Expected error after context cancellation, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("Expected context.Canceled error, got: %v", err)
	}
}

func TestScanWithStatsHandler(t *testing.T) {
	ctx := context.Background()
	c := gohbase.NewClient(*host)
	defer c.Close()

	// Prep data to scan over:
	q1, q2, q3 := "scanStats1", "scanStats2", "scanStats3"
	quals := []string{q1, q2, q3}
	fams := map[string][]string{"cf": quals}
	rows := 5
	for i := 0; i < rows; i++ {
		for _, q := range quals {
			err := insertKeyValueAtCol(c, strconv.Itoa(i), q, "cf", []byte(strconv.Itoa(i)),
				hrpc.Timestamp(time.UnixMilli(int64(i))))
			if err != nil {
				t.Fatalf("Failed to insert test data: %v", err)
			}
		}
	}

	t.Run("Basic scan test that calls handler", func(t *testing.T) {
		ss := &hrpc.ScanStats{}
		tn := []byte("handler table update")
		h := func(stats *hrpc.ScanStats) {
			ss.Table = tn
			t.Logf("Handler called, ScanStats = %s", stats)
		}

		scan, err := hrpc.NewScanRange(ctx, []byte(table), []byte(""), []byte("REVTEST-100"),
			hrpc.Families(fams),
			hrpc.WithScanStatsHandler(h))
		if err != nil {
			t.Fatal(err)
		}

		_, err = consumeScanner(c, scan)
		if err != nil {
			t.Fatalf("Error consuming scanner: %v", err)
		}

		if !bytes.Equal(ss.Table, tn) {
			t.Fatalf("expected table to be updated in ScanStats by handler to %v, got %v",
				tn, ss.Table)
		}

		if ss.ScanMetrics != nil {
			t.Fatalf("expected ScanMetrics to be nil, got %v", ss.ScanMetrics)
		}
	})

	t.Run("ScanStats with ScanMetrics", func(t *testing.T) {
		ss := &hrpc.ScanStats{}
		h := func(stats *hrpc.ScanStats) {
			ss = stats
			t.Logf("Handler called, ScanStats = %s", ss)
		}

		scan, err := hrpc.NewScanRange(ctx, []byte(table),
			// The table has been pre-split into multiple regions.
			// For testability, the scan range is defined here to scan within one region, as an
			// open scan across multiple regions will result in multiple calls to SendRPC, calling
			// the handler each time and making it difficult to isolate
			[]byte(""), []byte("REVTEST-100"),
			hrpc.Families(fams),
			hrpc.TimeRange(time.UnixMilli(0), time.UnixMilli(5)),
			// Both WithStatsHandler and WithTrackScanMetrics need to be set to collect ScanMetrics
			// in ScanStats
			hrpc.WithScanStatsHandler(h),
			hrpc.TrackScanMetrics())
		if err != nil {
			t.Fatal(err)
		}

		var rs []*hrpc.Result
		rs, err = consumeScanner(c, scan)
		if err != nil {
			t.Fatalf("Error consuming scanner: %v", err)
		}

		if ss.ScanMetrics == nil {
			t.Fatal("expected ScanStats to have ScanMetrics set, but got nil")
		}

		if ss.ScanMetrics["ROWS_SCANNED"]-ss.ScanMetrics["ROWS_FILTERED"] != int64(len(rs)) {
			t.Fatalf("Expected ScanMetrics to reflect %d rows returned to client.\n"+
				"ScanMetrics: %v,\nscan results: %v",
				len(rs), ss.ScanMetrics, rs)
		}
	})

	t.Run("ScanStats with scan over multiple regions for ScanStatsID",
		func(t *testing.T) {
			statsRes := make([]*hrpc.ScanStats, 0)
			h := func(stats *hrpc.ScanStats) {
				t.Logf("Handler called, ScanStats = %s", stats)
				statsRes = append(statsRes, stats)
			}

			// test table is pre-split to have multiple regions, so this scan will cover multiple
			// regions
			scan, err := hrpc.NewScan(ctx, []byte(table),
				hrpc.Families(fams),
				hrpc.WithScanStatsHandler(h))
			if err != nil {
				t.Fatal(err)
			}

			_, err = consumeScanner(c, scan)
			if err != nil {
				t.Fatalf("Error consuming scanner: %v", err)
			}

			// keySplits is used to split the table into pre-split regions. For this scan,
			// the number of ScanStats results (and hrpc.Results, although not tested here) should
			// equal about the num regions in the table.
			if len(statsRes) < len(keySplits)+1 {
				t.Fatalf("Expected handler to be called more than %d times, got %d calls",
					len(keySplits)+1, len(statsRes))
			}

			scanStatsID := statsRes[0].ScanStatsID
			for _, rs := range statsRes {
				if rs.ScanStatsID != scanStatsID {
					t.Fatalf("Expected ScanStatsID to be preserved in all sub-scans of"+
						"scanner, but was not - initial value %v, got %v",
						scanStatsID, rs.ScanStatsID)
				}
			}
		})
}

// consumeScanner is a helper function to consume the scanner and return the result.
func consumeScanner(c gohbase.Client, scan *hrpc.Scan) ([]*hrpc.Result, error) {
	sc := c.Scan(scan)
	defer sc.Close()

	rs := []*hrpc.Result{}

	for {
		var r *hrpc.Result
		r, err := sc.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return rs, err
		}
		rs = append(rs, r)
	}

	return rs, nil
}

func TestReplication(t *testing.T) {
	ctx := context.Background()
	ac := gohbase.NewAdminClient(*host)
	peerIdPrefix := table
	peerIdRegex := peerIdPrefix + ".*"

	t.Run("check no pre-existing peers for test table",
		func(t *testing.T) {
			listPeers, err := hrpc.NewListReplicationPeers(ctx,
				hrpc.ListReplicationPeersRegex(peerIdRegex))
			if err != nil {
				t.Fatal(err)
			}
			peers, err := ac.ListReplicationPeers(listPeers)
			if err != nil {
				t.Fatal(err)
			}
			if len(peers) != 0 {
				t.Fatalf("Expected 0 peers. Got %d", len(peers))
			}
		})

	// Cleanup after test
	defer func() {
		listPeers, err := hrpc.NewListReplicationPeers(ctx,
			hrpc.ListReplicationPeersRegex(peerIdRegex))
		if err != nil {
			t.Fatal(err)
		}
		peers, err := ac.ListReplicationPeers(listPeers)
		if err != nil {
			t.Fatal(err)
		}
		for _, peer := range peers {
			removePeer := hrpc.NewRemoveReplicationPeer(ctx, peer.GetId())
			err := ac.RemoveReplicationPeer(removePeer)
			if err != nil {
				t.Error(err)
			}
		}
	}()

	tcases := []struct {
		serial  bool
		enabled bool
	}{
		{
			serial:  false,
			enabled: true,
		},
		{
			serial:  false,
			enabled: false,
		},
		{
			serial:  true,
			enabled: true,
		},
		{
			serial:  true,
			enabled: false,
		},
	}
	for i, tc := range tcases {
		testNoStr := strconv.Itoa(i)
		t.Run("create list and invert enabled state of peer test #"+testNoStr, func(t *testing.T) {
			peerId := peerIdPrefix + "_" + testNoStr
			clusterKey := strings.Join([]string{
				*host, "2181",
				"/hbase-" + testNoStr,
			}, ":")
			tableBytes := []byte(table + "_" + testNoStr)
			columnFamilies := [][]byte{[]byte("cf"), []byte("cf" + testNoStr)}
			addPeer, err := hrpc.NewAddReplicationPeer(ctx, peerId, clusterKey,
				hrpc.AddReplicationPeerReplicateTableFamilies(tableBytes, columnFamilies...),
				hrpc.AddReplicationPeerSerial(tc.serial),
				hrpc.AddReplicationPeerEnabled(tc.enabled))
			if err != nil {
				t.Fatal(err)
			}
			err = ac.AddReplicationPeer(addPeer)
			if err != nil {
				t.Fatal(err)
			}
			listPeers, err := hrpc.NewListReplicationPeers(ctx,
				hrpc.ListReplicationPeersRegex(peerId))
			if err != nil {
				t.Fatal(err)
			}
			peers, err := ac.ListReplicationPeers(listPeers)
			if err != nil {
				t.Fatal(err)
			}
			if len(peers) != 1 {
				t.Fatalf("Expected 1 peers. Got %d", len(peers))
			}
			peer := peers[0]
			if peerId != peer.GetId() {
				t.Fatalf("Expected peerId %q. Got: %q", peerId, peer.GetId())
			}
			peerConfig := peer.GetConfig()
			peerReplicationState := peer.GetState().GetState()
			if clusterKey != peerConfig.GetClusterkey() {
				t.Fatalf("Expected clusterKey %q. Got: %q", clusterKey,
					peerConfig.GetClusterkey())
			}
			if (tc.enabled && peerReplicationState != pb.ReplicationState_ENABLED) ||
				(!tc.enabled && peerReplicationState != pb.ReplicationState_DISABLED) {
				t.Fatalf("Expected peer state %v. Got: %v", tc.enabled, peerReplicationState)
			}
			if tc.serial != peerConfig.GetSerial() {
				t.Fatalf("Expected peer serial state to be %v. Got: %v",
					tc.serial, peerConfig.GetSerial())
			}
			if peerConfig.GetReplicateAll() {
				t.Fatalf("Expected peer to set replicate_all=false. Got: %v",
					peerConfig.GetReplicateAll())
			}
			if len(peerConfig.GetTableCfs()) != 1 {
				t.Fatalf("Expected 1 table CF. Got: %d", len(peerConfig.GetTableCfs()))
			}
			tableCf := peerConfig.GetTableCfs()[0]
			if !bytes.Equal(tableCf.GetTableName().GetNamespace(), []byte("default")) ||
				!bytes.Equal(tableCf.GetTableName().GetQualifier(), tableBytes) {
				t.Fatalf("Expected table CF to contain table name %s:%s. Got: %s:%s",
					[]byte("default"), tableBytes, tableCf.GetTableName().GetNamespace(),
					tableCf.GetTableName().GetQualifier())
			}
			if len(tableCf.GetFamilies()) != len(columnFamilies) {
				t.Fatalf("Expected table CF to contain %d families. Got: %d",
					len(columnFamilies), len(tableCf.GetFamilies()))
			}
			for j, tableCfFamily := range tableCf.GetFamilies() {
				family := columnFamilies[j]
				if !bytes.Equal(family, tableCfFamily) {
					t.Fatalf("Expected table CF to contain family %v at position %d. Got: %v",
						family, j, tableCfFamily)
				}
			}

			if tc.enabled {
				// disable enabled peer
				disablePeer := hrpc.NewDisableReplicationPeer(ctx, peerId)
				err := ac.DisableReplicationPeer(disablePeer)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				// enable disabled peer
				enablePeer := hrpc.NewEnableReplicationPeer(ctx, peerId)
				err := ac.EnableReplicationPeer(enablePeer)
				if err != nil {
					t.Fatal(err)
				}
			}

			listPeers, err = hrpc.NewListReplicationPeers(ctx,
				hrpc.ListReplicationPeersRegex(peerId))
			if err != nil {
				t.Fatal(err)
			}
			peers, err = ac.ListReplicationPeers(listPeers)
			if err != nil {
				t.Fatal(err)
			}
			if len(peers) != 1 {
				t.Fatalf("Expected 1 peers. Got %d", len(peers))
			}
			peer = peers[0]
			peerReplicationState = peer.GetState().GetState()
			if (tc.enabled && peerReplicationState != pb.ReplicationState_DISABLED) ||
				(!tc.enabled && peerReplicationState != pb.ReplicationState_ENABLED) {
				t.Fatalf("Expected peer state %v. Got: %v", !tc.enabled, peerReplicationState)
			}
		})
	}

	t.Run("check regex peer listing",
		func(t *testing.T) {
			listPeers, err := hrpc.NewListReplicationPeers(ctx,
				hrpc.ListReplicationPeersRegex(peerIdRegex))
			if err != nil {
				t.Fatal(err)
			}
			peers, err := ac.ListReplicationPeers(listPeers)
			if err != nil {
				t.Fatal(err)
			}
			if len(peers) != len(tcases) {
				t.Fatalf("Expected %d peers. Got %d", len(tcases), len(peers))
			}
		})
}
