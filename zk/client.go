// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package zk

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/tsuna/gohbase/pb"
)

const (
	sessionTimeout = 30

	znode = "/hbase"
)

func LocateMeta(zkquorum string) (string, uint16, error) {
	zks := strings.Split(zkquorum, ",")
	zkconn, _, err := zk.Connect(zks, time.Duration(sessionTimeout)*time.Second)
	if err != nil {
		return "", 0,
			fmt.Errorf("Error connecting to ZooKeeper at %v: %s", zks, err)
	}
	buf, _, err := zkconn.Get(znode + "/meta-region-server")
	if err != nil {
		return "", 0,
			fmt.Errorf("Failed to read the meta-region-server znode: %s", err)
	}
	zkconn.Close()
	if len(buf) == 0 {
		log.Fatal("meta-region-server was empty!")
	} else if buf[0] != 0xFF {
		return "", 0,
			fmt.Errorf("The first byte of meta-region-server was 0x%x, not 0xFF", buf[0])
	}
	metadataLen := binary.BigEndian.Uint32(buf[1:])
	if metadataLen < 1 || metadataLen > 65000 {
		return "", 0, fmt.Errorf("Invalid metadata length: %d", metadataLen)
	}
	buf = buf[1+4+metadataLen:]
	magic := binary.BigEndian.Uint32(buf)
	const pbufMagic = 1346524486 // 4 bytes: "PBUF"
	if magic != pbufMagic {
		return "", 0, fmt.Errorf("Invalid magic number: %d", magic)
	}
	buf = buf[4:]
	meta := &pb.MetaRegionServer{}
	err = proto.UnmarshalMerge(buf, meta)
	if err != nil {
		return "", 0,
			fmt.Errorf("Failed to deserialize the MetaRegionServer entry from ZK: %s", err)
	}
	server := meta.Server
	return *server.HostName, uint16(*server.Port), nil
}
