// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

//go:build integration

package gohbase_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

// Name of the meta region.
const metaTableName = "hbase:meta"

// Info family
var infoFamily = map[string][]string{
	"info": nil,
}

var cFamilies = map[string]map[string]string{
	"cf": nil,
	"cf2": {
		"MIN_VERSIONS": "1",
	},
}

func TestCreateTable(t *testing.T) {
	testTableName := t.Name() + "_" + getTimestampString()
	t.Log("testTableName=" + testTableName)

	ac := gohbase.NewAdminClient(*host)
	crt := hrpc.NewCreateTable(context.Background(), []byte(testTableName), cFamilies)

	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	defer func() {
		err := DeleteTable(ac, testTableName)
		if err != nil {
			panic(err)
		}
	}()

	rsp, err := getTableRegions(testTableName)
	if err != nil {
		t.Fatal(err)
	}
	if len(rsp) != 1 {
		t.Errorf("Meta returned %d rows for table '%s' , want 1", len(rsp), testTableName)
	}
}

// getTableRegions is a helper to scan hbase:meta for all regions of a table, tableName
func getTableRegions(tableName string) ([]*hrpc.Result, error) {
	c := gohbase.NewClient(*host)
	metaKeyStart := tableName + ",,"
	metaKeyStop := tableName + ";"
	scan, err := hrpc.NewScanRangeStr(context.Background(), metaTableName,
		metaKeyStart, metaKeyStop)
	if err != nil {
		return nil, fmt.Errorf("failed to create Scan request: %w", err)
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
			return nil, fmt.Errorf("error consuming scanner: %w", err)
		}
		rsp = append(rsp, res)
	}
	return rsp, nil
}

func TestCreatePresplitTable(t *testing.T) {
	testTableName := t.Name() + "_" + getTimestampString()
	t.Log("testTableName=" + testTableName)

	ac := gohbase.NewAdminClient(*host)
	// Keys are split points, so expect split keys + 1 regions
	splitkeys := [][]byte{
		[]byte{3},
		[]byte("foo"),
		[]byte("wow"),
	}
	crt := hrpc.NewCreateTable(context.Background(), []byte(testTableName),
		cFamilies, hrpc.SplitKeys(splitkeys))

	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	defer func() {
		err := DeleteTable(ac, testTableName)
		if err != nil {
			panic(err)
		}
	}()

	rsp, err := getTableRegions(testTableName)
	if err != nil {
		t.Fatal(err)
	}

	if len(rsp) != len(splitkeys)+1 {
		t.Errorf("Meta returned %d rows for table '%s' , want %d",
			len(rsp), testTableName, len(splitkeys)+1)
	}
}

func TestCreateTableWithAttributes(t *testing.T) {
	testTableName := t.Name() + "_" + getTimestampString()
	t.Log("testTableName=" + testTableName)

	attrs := map[string]string{
		"NORMALIZATION_ENABLED": "TRUE",
	}

	ac := gohbase.NewAdminClient(*host)
	crt := hrpc.NewCreateTable(
		context.Background(),
		[]byte(testTableName),
		cFamilies,
		hrpc.TableAttributes(attrs),
	)

	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	defer func() {
		err := DeleteTable(ac, testTableName)
		if err != nil {
			panic(err)
		}
	}()

	rsp, err := getTableRegions(testTableName)
	if err != nil {
		t.Fatal(err)
	}

	if len(rsp) != 1 {
		t.Errorf("Meta returned %d rows for table '%s' , want 1", len(rsp), testTableName)
	}
}

func TestDisableDeleteTable(t *testing.T) {
	testTableName := t.Name() + "_" + getTimestampString()
	t.Log("testTableName=" + testTableName)
	ac := gohbase.NewAdminClient(*host)

	crt := hrpc.NewCreateTable(context.Background(), []byte(testTableName), cFamilies)
	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	// disable
	dit := hrpc.NewDisableTable(context.Background(), []byte(testTableName))
	err := ac.DisableTable(dit)
	if err != nil {
		t.Errorf("DisableTable returned an error: %v", err)
	}

	// HBase protobuf will error if testTableName wasn't disabled before deletion
	det := hrpc.NewDeleteTable(context.Background(), []byte(testTableName))
	err = ac.DeleteTable(det)
	if err != nil {
		t.Errorf("DeleteTable returned an error: %v", err)
	}

	rsp, err := getTableRegions(testTableName)
	if err != nil {
		t.Fatal(err)
	}

	if len(rsp) != 0 {
		t.Errorf("Meta returned %d rows for table '%s', want 0", len(rsp), testTableName)
	}
}

func TestEnableTable(t *testing.T) {
	testTableName := t.Name() + "_" + getTimestampString()
	t.Log("testTableName=" + testTableName)
	ac := gohbase.NewAdminClient(*host)

	crt := hrpc.NewCreateTable(context.Background(), []byte(testTableName), cFamilies)
	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	defer func() {
		err := DeleteTable(ac, testTableName)
		if err != nil {
			panic(err)
		}
	}()

	// disable
	dit := hrpc.NewDisableTable(context.Background(), []byte(testTableName))
	err := ac.DisableTable(dit)
	if err != nil {
		t.Errorf("DisableTable returned an error: %v", err)
	}

	et := hrpc.NewEnableTable(context.Background(), []byte(testTableName))
	err = ac.EnableTable(et)
	if err != nil {
		t.Errorf("EnableTable returned an error: %v", err)
	}

	delt := hrpc.NewDeleteTable(context.Background(), []byte(testTableName))
	err = ac.DeleteTable(delt)
	if err == nil || !strings.Contains(err.Error(), "TableNotDisabledException") {
		t.Errorf("DeleteTable should error with TableNotDisabledException, got %s", err)
	}
}

func getTimestampString() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
