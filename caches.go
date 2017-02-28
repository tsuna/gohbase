// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"io"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/cznic/b"
	"github.com/tsuna/gohbase/hrpc"
)

// clientRegionCache is client -> region cache. Used to quickly
// look up all the regioninfos that map to a specific client
type clientRegionCache struct {
	m sync.Mutex

	regions map[hrpc.RegionClient]map[hrpc.RegionInfo]struct{}
}

// put caches client and associates a region with it. Returns a client that is in cache.
// TODO: obvious place for optimization (use map with address as key to lookup exisiting clients)
func (rcc *clientRegionCache) put(c hrpc.RegionClient, r hrpc.RegionInfo) hrpc.RegionClient {
	rcc.m.Lock()
	for existingClient, regions := range rcc.regions {
		// check if client already exists, checking by host and port
		// because concurrent callers might try to put the same client
		if c.Host() == existingClient.Host() && c.Port() == existingClient.Port() {
			// check client already knows about the region, checking
			// by pointer is enough because we make sure that there are
			// no regions with the same name around
			if _, ok := regions[r]; !ok {
				regions[r] = struct{}{}
			}
			rcc.m.Unlock()

			log.WithFields(log.Fields{
				"existingClient": existingClient,
				"client":         c,
			}).Debug("region client is already in client's cache")
			return existingClient
		}
	}

	// no such client yet
	rcc.regions[c] = map[hrpc.RegionInfo]struct{}{r: struct{}{}}
	rcc.m.Unlock()

	log.WithField("client", c).Info("added new region client")
	return c
}

func (rcc *clientRegionCache) del(r hrpc.RegionInfo) {
	rcc.m.Lock()
	c := r.Client()
	if c != nil {
		r.SetClient(nil)

		regions := rcc.regions[c]
		delete(regions, r)

		if len(regions) == 0 {
			// close region client if noone is using it
			delete(rcc.regions, c)
			c.Close()
		}
	}
	rcc.m.Unlock()
}

func (rcc *clientRegionCache) closeAll() {
	rcc.m.Lock()
	for client, regions := range rcc.regions {
		for region := range regions {
			region.MarkUnavailable()
			region.SetClient(nil)
		}
		client.Close()
	}
	rcc.m.Unlock()
}

func (rcc *clientRegionCache) clientDown(c hrpc.RegionClient) map[hrpc.RegionInfo]struct{} {
	rcc.m.Lock()
	downregions, ok := rcc.regions[c]
	delete(rcc.regions, c)
	rcc.m.Unlock()

	if ok {
		log.WithField("client", c).Info("removed region client")
	}
	return downregions
}

// TODO: obvious place for optimization (use map with address as key to lookup exisiting clients)
func (rcc *clientRegionCache) checkForClient(host string, port uint16) hrpc.RegionClient {
	rcc.m.Lock()
	defer rcc.m.Unlock()

	for client := range rcc.regions {
		if client.Host() == host && client.Port() == port {
			return client
		}
	}
	return nil
}

// key -> region cache.
type keyRegionCache struct {
	m sync.Mutex

	// Maps a []byte of a region start key to a hrpc.RegionInfo
	regions *b.Tree
}

func (krc *keyRegionCache) get(key []byte) ([]byte, hrpc.RegionInfo) {
	// When seeking - "The Enumerator's position is possibly after the last item in the tree"
	// http://godoc.org/github.com/cznic/b#Tree.Set
	krc.m.Lock()

	enum, ok := krc.regions.Seek(key)
	k, v, err := enum.Prev()
	if err == io.EOF && krc.regions.Len() > 0 {
		// We're past the end of the tree. Return the last element instead.
		// (Without this code we always get a cache miss and create a new client for each req.)
		k, v = krc.regions.Last()
		err = nil
	} else if !ok {
		k, v, err = enum.Prev()
	}
	enum.Close()
	if err != nil {
		krc.m.Unlock()
		return nil, nil
	}
	krc.m.Unlock()
	return k.([]byte), v.(hrpc.RegionInfo)
}

func isRegionOverlap(regA, regB hrpc.RegionInfo) bool {
	// if region's stop key is empty, it's assumed to be the greatest key
	return bytes.Equal(regA.Namespace(), regB.Namespace()) &&
		bytes.Equal(regA.Table(), regB.Table()) &&
		(len(regB.StopKey()) == 0 || bytes.Compare(regA.StartKey(), regB.StopKey()) < 0) &&
		(len(regA.StopKey()) == 0 || bytes.Compare(regA.StopKey(), regB.StartKey()) > 0)
}

func (krc *keyRegionCache) getOverlaps(reg hrpc.RegionInfo) []hrpc.RegionInfo {
	var overlaps []hrpc.RegionInfo
	var v interface{}
	var err error

	// deal with empty tree in the beginning so that we don't have to check
	// EOF errors for enum later
	if krc.regions.Len() == 0 {
		return overlaps
	}

	enum, ok := krc.regions.Seek(reg.Name())
	if !ok {
		// need to check if there are overlaps before what we found
		_, _, err = enum.Prev()
		if err == io.EOF {
			// we are in the end of tree, get last entry
			_, v = krc.regions.Last()
			currReg := v.(hrpc.RegionInfo)
			if isRegionOverlap(currReg, reg) {
				return append(overlaps, currReg)
			}
		} else {
			_, v, err = enum.Next()
			if err == io.EOF {
				// we are before the beginning of the tree now, get new enum
				enum.Close()
				enum, err = krc.regions.SeekFirst()
			} else {
				// otherwise, check for overlap before us
				currReg := v.(hrpc.RegionInfo)
				if isRegionOverlap(currReg, reg) {
					overlaps = append(overlaps, currReg)
				}
			}
		}
	}

	// now append all regions that overlap until the end of the tree
	// or until they don't overlap
	_, v, err = enum.Next()
	for err == nil && isRegionOverlap(v.(hrpc.RegionInfo), reg) {
		overlaps = append(overlaps, v.(hrpc.RegionInfo))
		_, v, err = enum.Next()
	}
	enum.Close()
	return overlaps
}

// put looks up if there's already region with this name in regions cache
// and if there's, returns it in overlaps and doesn't modify the cache.
// Otherwise, it puts the region and removes all overlaps in case all of
// them are older. Returns a slice of overlapping regions and whether
// passed region was put in the cache.
func (krc *keyRegionCache) put(reg hrpc.RegionInfo) (overlaps []hrpc.RegionInfo, replaced bool) {
	krc.m.Lock()
	krc.regions.Put(reg.Name(), func(v interface{}, exists bool) (interface{}, bool) {
		if exists {
			// region is already in cache,
			// note: regions with the same name have the same age
			overlaps = []hrpc.RegionInfo{v.(hrpc.RegionInfo)}
			return nil, false
		}
		// find all entries that are overlapping with the range of the new region.
		overlaps = krc.getOverlaps(reg)
		for _, o := range overlaps {
			if o.ID() > reg.ID() {
				// overlapping region is younger,
				// don't replace any regions
				// TODO: figure out if there can a case where we might
				// have both older and younger overlapping regions, for
				// now we only replace if all overlaps are older
				return nil, false
			}
		}
		// all overlaps are older, put the new region
		replaced = true
		return reg, true
	})
	if !replaced {
		krc.m.Unlock()

		log.WithFields(log.Fields{
			"region":   reg,
			"overlaps": overlaps,
			"replaced": replaced,
		}).Debug("region is already in cache")
		return
	}
	// delete overlapping regions
	// TODO: in case overlaps are always either younger or older,
	// we can just greedily remove them in Put function
	for _, o := range overlaps {
		krc.regions.Delete(o.Name())
		// let region establishers know that they can give up
		o.MarkDead()
	}
	krc.m.Unlock()

	log.WithFields(log.Fields{
		"region":   reg,
		"overlaps": overlaps,
		"replaced": replaced,
	}).Info("added new region")
	return
}

func (krc *keyRegionCache) del(reg hrpc.RegionInfo) bool {
	krc.m.Lock()
	success := krc.regions.Delete(reg.Name())
	krc.m.Unlock()
	// let region establishers know that they can give up
	reg.MarkDead()

	log.WithFields(log.Fields{
		"region": reg,
	}).Debug("removed region")
	return success
}
