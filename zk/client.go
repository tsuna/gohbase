// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package zk encapsulates our interactions with ZooKeeper.
package zk

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/tsuna/gohbase/internal/pb"
)

type logger struct{}

func (l *logger) Printf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func init() {
	zk.DefaultLogger = &logger{}
}

// ResourceName is a type alias that is used to represent different resources
// in ZooKeeper
type ResourceName string

const (
	sessionTimeout = 30

	// Meta is a ResourceName that indicates that the location of the Meta
	// table is what will be fetched
	MetaPath = "/meta-region-server"

	// Master is a ResourceName that indicates that the location of the Master
	// server is what will be fetched
	MasterPath = "/master"

	DefaultRoot = "/hbase"

	Master = ResourceName(DefaultRoot + MasterPath)
	Meta = ResourceName(DefaultRoot + MetaPath)
)

// Client is an interface of client that retrieves meta infomation from zookeeper
type Client interface {
	LocateResource(ResourceName) (string, uint16, error)
}

type client struct {
	zks []string
}

// NewClient establishes connection to zookeeper and returns the client
func NewClient(zkquorum string) Client {
	return &client{
		zks: strings.Split(zkquorum, ","),
	}
}

// LocateResource returns the location of the specified resource.
func (c *client) LocateResource(resource ResourceName) (string, uint16, error) {
	conn, _, err := zk.Connect(c.zks, time.Duration(sessionTimeout)*time.Second)
	if err != nil {
		return "", 0, fmt.Errorf("error connecting to ZooKeeper at %v: %s", c.zks, err)
	}
	defer conn.Close()

	buf, _, err := conn.Get(string(resource))
	if err != nil {
		return "", 0,
			fmt.Errorf("failed to read the %s znode: %s", resource, err)
	}
	if len(buf) == 0 {
		log.Fatalf("%s was empty!", resource)
	} else if buf[0] != 0xFF {
		return "", 0,
			fmt.Errorf("the first byte of %s was 0x%x, not 0xFF", resource, buf[0])
	}
	metadataLen := binary.BigEndian.Uint32(buf[1:])
	if metadataLen < 1 || metadataLen > 65000 {
		return "", 0, fmt.Errorf("invalid metadata length for %s: %d", resource, metadataLen)
	}
	buf = buf[1+4+metadataLen:]
	magic := binary.BigEndian.Uint32(buf)
	const pbufMagic = 1346524486 // 4 bytes: "PBUF"
	if magic != pbufMagic {
		return "", 0, fmt.Errorf("invalid magic number for %s: %d", resource, magic)
	}
	buf = buf[4:]
	var server *pb.ServerName
	if strings.HasSuffix(string(resource), MetaPath) {
		meta := &pb.MetaRegionServer{}
		err = proto.UnmarshalMerge(buf, meta)
		if err != nil {
			return "", 0,
				fmt.Errorf("failed to deserialize the MetaRegionServer entry from ZK: %s", err)
		}
		server = meta.Server
	} else {
		master := &pb.Master{}
		err = proto.UnmarshalMerge(buf, master)
		if err != nil {
			return "", 0,
				fmt.Errorf("failed to deserialize the Master entry from ZK: %s", err)
		}
		server = master.Master
	}
	return *server.HostName, uint16(*server.Port), nil
}
