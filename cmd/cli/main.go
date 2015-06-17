// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package main

import (
	"flag"
	"log"

	"github.com/tsuna/gohbase"
	"golang.org/x/net/context"
)

var zkquorum = flag.String("zkquorum", "localhost",
	"Specification of the ZooKeeper quorum")

func main() {
	client := gohbase.NewClient(*zkquorum)
	resp, err := client.CheckTable(context.Background(), "aeris")
	if err != nil {
		log.Fatalf("Fail: %s", err)
	}
	log.Printf("get returned: %s", resp)
}
