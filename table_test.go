// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build integration

package gohbase_test

import (
	"strings"
	"testing"
	"time"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

// Name of the meta region.
const metaTableName = "hbase:meta"

// Info family
var infoFamily = map[string][]string{
	"info": nil,
}

func TestCreateTable(t *testing.T) {
	testTableName := "test1_" + getTimestampString()
	t.Log("testTableName=" + testTableName)

	ac := gohbase.NewAdminClient(*host)
	crt, err := hrpc.NewCreateTable(context.Background(), []byte(testTableName),
		[]string{"cf", "cf2"})
	if err != nil {
		t.Errorf("NewCreateTable returned an error: %s", err)
	}

	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	// check in hbase:meta if there's a region for the table
	c := gohbase.NewClient(*host)
	metaKey := testTableName + ",,"
	keyFilter := filter.NewPrefixFilter([]byte(metaKey))
	scan, err := hrpc.NewScanStr(context.Background(), metaTableName, hrpc.Filters(keyFilter))
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	rsp, err := c.Scan(scan)
	if err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}

	if len(rsp) != 1 {
		t.Errorf("Meta returned %s rows for prefix '%s' , want 1", len(rsp), metaKey)
	}
}

func TestDisableDeleteTable(t *testing.T) {
	testTableName := "test1_" + getTimestampString()
	t.Log("testTableName=" + testTableName)
	ac := gohbase.NewAdminClient(*host)

	crt, err := hrpc.NewCreateTable(context.Background(), []byte(testTableName),
		[]string{"cf", "cf2"})
	if err != nil {
		t.Errorf("NewCreateTable returned an error: %s", err)
	}

	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	// disable
	dit := hrpc.NewDisableTable(context.Background(), []byte(testTableName))
	err = ac.DisableTable(dit)
	if err != nil {
		t.Errorf("DisableTable returned an error: %v", err)
	}

	// HBase protobuf will error if testTableName wasn't disabled before deletion
	det := hrpc.NewDeleteTable(context.Background(), []byte(testTableName))
	err = ac.DeleteTable(det)
	if err != nil {
		t.Errorf("DeleteTable returned an error: %v", err)
	}

	// check in hbase:meta if there's a region for the table
	c := gohbase.NewClient(*host)
	metaKey := testTableName + ",,"
	keyFilter := filter.NewPrefixFilter([]byte(metaKey))
	scan, err := hrpc.NewScanStr(context.Background(), metaTableName, hrpc.Filters(keyFilter))
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	rsp, err := c.Scan(scan)
	if err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}

	if len(rsp) != 0 {
		t.Errorf("Meta returned %s rows for prefix '%s' , want 0", len(rsp), metaKey)
	}
}

func TestEnableTable(t *testing.T) {
	testTableName := "test1_" + getTimestampString()
	t.Log("testTableName=" + testTableName)
	ac := gohbase.NewAdminClient(*host)

	crt, err := hrpc.NewCreateTable(context.Background(), []byte(testTableName),
		[]string{"cf", "cf2"})
	if err != nil {
		t.Errorf("NewCreateTable returned an error: %s", err)
	}

	if err := ac.CreateTable(crt); err != nil {
		t.Errorf("CreateTable returned an error: %v", err)
	}

	// disable
	dit := hrpc.NewDisableTable(context.Background(), []byte(testTableName))
	err = ac.DisableTable(dit)
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
	return time.Now().Format("20060102150405")
}
