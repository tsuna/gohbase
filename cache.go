// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"io"
	"sync"

	"github.com/cznic/b"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/regioninfo"
)

// region -> client cache.
type regionClientCache struct {
	m sync.Mutex

	clients map[*regioninfo.Info]*region.Client
}

func (rcc *regionClientCache) get(r *regioninfo.Info) *region.Client {
	rcc.m.Lock()
	c := rcc.clients[r]
	rcc.m.Unlock()
	return c
}

func (rcc *regionClientCache) put(r *regioninfo.Info, c *region.Client) {
	rcc.m.Lock()
	rcc.clients[r] = c
	rcc.m.Unlock()
}

// key -> region cache.
type keyRegionCache struct {
	m sync.Mutex

	// Maps a []byte of a region start key to a *regioninfo.Info
	regions *b.Tree
}

func (krc *keyRegionCache) get(key []byte) ([]byte, *regioninfo.Info) {
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
	// TODO: It would be nice if we could do just enum.Get() to avoid the
	// unnecessary cost of seeking to the next entry.
	krc.m.Unlock()
	if err != nil {
		return nil, nil
	}
	return k.([]byte), v.(*regioninfo.Info)
}

func (krc *keyRegionCache) put(key []byte, reg *regioninfo.Info) *regioninfo.Info {
	krc.m.Lock()
	oldV, _ := krc.regions.Put(key, func(interface{}, bool) (interface{}, bool) { return reg, true })
	krc.m.Unlock()
	if oldV == nil {
		return nil
	}
	return oldV.(*regioninfo.Info)
}
