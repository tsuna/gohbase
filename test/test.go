// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package test

import (
	"os"
	"os/exec"
	"strings"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

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
