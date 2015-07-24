// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/regioninfo"
	"github.com/tsuna/gohbase/zk"
	"golang.org/x/net/context"
)

type AdminClient struct {
	masterInfo   *regioninfo.Info
	masterClient *region.Client
	zkquorum     string
}

func NewAdminClient(zkquorum string) *AdminClient {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new admin client.")

	a := &AdminClient{
		zkquorum:   zkquorum,
		masterInfo: &regioninfo.Info{},
	}
	return a
}

func (a *AdminClient) CreateTable(ct *hrpc.CreateTable) (*pb.CreateTableResponse, error) {
	resp, err := a.sendRPC(ct)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.CreateTableResponse), err
}

func (a *AdminClient) DisableTable(ct *hrpc.DisableTable) (*pb.DisableTableResponse, error) {
	resp, err := a.sendRPC(ct)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.DisableTableResponse), err
}

func (a *AdminClient) DeleteTable(ct *hrpc.DeleteTable) (*pb.DeleteTableResponse, error) {
	resp, err := a.sendRPC(ct)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.DeleteTableResponse), err
}

func (a *AdminClient) sendRPC(rpc hrpc.Call) (proto.Message, error) {
	log.WithFields(log.Fields{
		"RPC Name": rpc.GetName(),
	}).Debug("Sending RPC to Master")
	err := a.queueRPC(rpc)
	if err == ErrDeadline {
		return nil, err
	}

	if err == nil {
		var res hrpc.RPCResult
		resch := rpc.GetResultChan()

		select {
		case res = <-resch:
		case <-rpc.GetContext().Done():
			return nil, ErrDeadline
		}

		if _, ok := err.(region.RetryableError); ok {
			return a.sendRPC(rpc)
		} else if _, ok := err.(region.UnrecoverableError); ok {
			// Prevents dropping into the else block below,
			// error handling happens a few lines down
		} else {
			log.WithFields(log.Fields{
				"Result": res.Msg,
				"Error":  err,
			}).Debug("Successfully sent RPC to Master. Returning.")
			return res.Msg, res.Error
		}
	}

	succ := a.masterInfo.MarkUnavailable()
	if succ {
		go a.reestablishMaster()
	}
	return a.sendRPC(rpc)
}

func (a *AdminClient) queueRPC(rpc hrpc.Call) error {
	if a.masterClient == nil {
		succ := a.masterInfo.MarkUnavailable()
		if succ {
			go a.reestablishMaster()
		}
	}
	ch := a.masterInfo.GetAvailabilityChan()
	if ch != nil {
		select {
		case <-ch:
			return a.queueRPC(rpc)
		case <-rpc.GetContext().Done():
			return ErrDeadline
		}
	}

	return a.masterClient.QueueRPC(rpc)
}

func (a *AdminClient) reestablishMaster() {
	log.Warn("Attempting to re-establish master.")
	backoffAmount := 16 * time.Millisecond
	for {
		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)

		ret := make(chan error)
		go a.locateMaster(ret)

		var err error
		select {
		case err = <-ret:
		case <-ctx.Done():
			continue
		}

		if err == nil {
			a.masterInfo.MarkAvailable()
			return
		}

		time.Sleep(time.Duration(backoffAmount))
		if backoffAmount < 5000*time.Millisecond {
			backoffAmount *= 2
		} else {
			backoffAmount += 5000 * time.Millisecond
		}
	}
}

// Looks up the master server in ZooKeeper.
func (a *AdminClient) locateMaster(ret chan error) {
	host, port, err := zk.LocateResource(a.zkquorum, zk.Master)
	if err != nil {
		log.Printf("Error while locating master: %s", err)
		ret <- err
		return
	}
	log.WithFields(log.Fields{
		"Host": host,
		"Port": port,
	}).Debug("Located MASTER from ZooKeeper")
	a.masterClient, err = region.NewClient(host, port, region.MasterClient, 0, time.Second)
	ret <- err
}
