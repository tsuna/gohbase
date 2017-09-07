// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build integration

package gohbase_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"math"

	log "github.com/Sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
)

var host = flag.String("host", "localhost", "The location where HBase is running")

const table = "test1"

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
	ct := hrpc.NewCreateTable(context.Background(), []byte(table), cf)
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

// LaunchRegionServers uses the script local-regionservers.sh to create new
// RegionServers. Fails silently if server already exists.
// Ex. LaunchRegions([]string{"2", "3"}) launches two servers with id=2,3
func LaunchRegionServers(servers []string) {
	hh := os.Getenv("HBASE_HOME")
	servers = append([]string{"start"}, servers...)
	exec.Command(hh+"/bin/local-regionservers.sh", servers...).Run()
}

// StopRegionServers uses the script local-regionservers.sh to stop existing
// RegionServers. Fails silently if server isn't running.
func StopRegionServers(servers []string) {
	hh := os.Getenv("HBASE_HOME")
	servers = append([]string{"stop"}, servers...)
	exec.Command(hh+"/bin/local-regionservers.sh", servers...).Run()
}
func TestMain(m *testing.M) {
	flag.Parse()

	if host == nil {
		panic("Host is not set!")
	}

	log.SetLevel(log.DebugLevel)

	ac := gohbase.NewAdminClient(*host)

	var err error
	for {
		err = CreateTable(ac, table, []string{"cf", "cf2"})
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

//Test retrieval of cluster status
func TestClusterStatus(t *testing.T) {
	ac := gohbase.NewAdminClient(*host)
	defer ac.(gohbase.Client).Close()

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

	ctx, _ := context.WithTimeout(context.Background(), 0)
	get, err = hrpc.NewGetStr(ctx, table, key, hrpc.Families(headers))
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	_, err = c.Get(get)
	if err != gohbase.ErrDeadline {
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
		t.Fatal("NewGetStr returned an error: %v", err)
	}
	_, err = c.Get(get)
	if err != gohbase.TableNotFound {
		t.Errorf("Get returned unexpected error: %v", err)
	}
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		t.Fatal("NewPutStr returned an error: %v", err)
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
	key := "row2"
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	if host == nil {
		t.Fatal("Host is not set!")
	}
	c := gohbase.NewClient(*host)
	defer c.Close()
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("NewPutStr returned an error: %v", err)
	}
	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)
	putRequest, err = hrpc.NewPutStr(ctx, table, key, values)
	_, err = c.Put(putRequest)
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

func TestDeleteTimestamp(t *testing.T) {
	key := "TestDeleteTimestamp"
	c := gohbase.NewClient(*host)
	defer c.Close()
	var putTs uint64 = 50
	timestamp := time.Unix(0, int64(putTs*1e6))
	err := insertKeyValue(c, key, "cf", []byte("1"), hrpc.Timestamp(timestamp))
	if err != nil {
		t.Fatalf("Put failed: %s", err)
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), table, key,
		map[string]map[string][]byte{"cf": map[string][]byte{"a": nil}},
		hrpc.Timestamp(timestamp))
	_, err = c.Delete(deleteRequest)
	if err != nil {
		t.Fatalf("Delete failed: %s", err)
	}
	get, err := hrpc.NewGetStr(context.Background(), table, key,
		hrpc.Families(map[string][]string{"cf": nil}))
	rsp, err := c.Get(get)
	if err != nil {
		t.Fatalf("Get failed: %s", err)
	}
	if len(rsp.Cells) != 0 {
		t.Errorf("Timestamp wasn't deleted, get result length: %d", len(rsp.Cells))
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
	scan1 := *rsp[0].Cells[0]
	if string(scan1.Row) != "TestScanTimeRangeVersions1" && *scan1.Timestamp != 51 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			51, *scan1.Timestamp)
	}
	scan2 := *rsp[0].Cells[1]
	if string(scan2.Row) != "TestScanTimeRangeVersions1" && *scan2.Timestamp != 50 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			50, *scan2.Timestamp)
	}
	if uint32(len(rsp[1].Cells)) != maxVersions {
		t.Fatalf("Expected versions: %d, Got versions: %d", maxVersions, len(rsp[1].Cells))
	}
	scan3 := *rsp[1].Cells[0]
	if string(scan3.Row) != "TestScanTimeRangeVersions2" && *scan3.Timestamp != 52 {
		t.Errorf("Timestamps are not the same. Expected Time: %v, Got Time: %v",
			52, *scan3.Timestamp)
	}
	scan4 := *rsp[1].Cells[1]
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
	key := "TestScannerClose"
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
	key := "TestScanner"
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

	// make sure we get io.EOF eventually
	for {
		if _, err = scanner.Next(); err == io.EOF {
			break
		}
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
		putRequest1, err := hrpc.NewPutStr(context.Background(), table, keyPrefix+string(i), values)
		if err != nil {
			t.Fatalf("NewPutStr returned an error: %v", err)
		}
		putRequest2, err := hrpc.NewPutStr(context.Background(), table, keyPrefix+string(i), values)
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
	LaunchRegionServers([]string{"2", "3"})

	// Now (gracefully) stop servers 1,2.
	// All regions should now be on server 3.
	StopRegionServers([]string{"1", "2"})
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

	// Clean up by re-launching RS1 and closing RS3
	LaunchRegionServers([]string{"1"})
	StopRegionServers([]string{"3"})
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
	values := map[string]map[string][]byte{columnFamily: map[string][]byte{}}
	values[columnFamily]["a"] = value
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values, options...)
	if err != nil {
		return err
	}
	_, err = c.Put(putRequest)
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
	if resultCnt != 1 {
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
