// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// To run this command you need protoc.
//go:generate go install github.com/golang/protobuf/protoc-gen-go
//go:generate protoc --proto_path=. --go_out=. Cell.proto Client.proto ClusterId.proto ClusterStatus.proto Comparator.proto ErrorHandling.proto FS.proto Filter.proto HBase.proto Master.proto Procedure.proto Quota.proto RPC.proto Tracing.proto ZooKeeper.proto
// brew install protobuf

package pb
