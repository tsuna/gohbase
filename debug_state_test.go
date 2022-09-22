package gohbase

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/region"
)

func TestDebugStateSanity(t *testing.T) {
	client := newClient("~invalid.quorum~") // We shouldn't connect to ZK.

	reg := client.getRegionFromCache([]byte("test"), []byte("theKey"))
	if reg != nil {
		t.Errorf("Found region %v even though the cache was empty?!", reg)
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	regClientAddr := "regionserver:1"
	regClient := region.NewClient(
		regClientAddr,
		region.RegionClient,
		defaultRPCQueueSize,
		defaultFlushInterval,
		defaultEffectiveUser,
		region.DefaultReadTimeout,
		client.compressionCodec,
	)
	newClientFn := func() hrpc.RegionClient {
		return regClient
	}

	// Inject 3 entries in the cache.
	region1 := region.NewInfo(
		1,
		nil,
		[]byte("test"),
		[]byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		[]byte(""),
		[]byte("foo"),
	)
	if os, replaced := client.regions.put(region1); !replaced {
		t.Errorf("Expected to put new region into cache, got: %v", os)
	} else if len(os) != 0 {
		t.Errorf("Didn't expect any overlaps, got: %v", os)
	}
	region1.SetClient(regClient)
	client.clients.put("regionserver:1", region1, newClientFn)

	region2 := region.NewInfo(
		2,
		nil,
		[]byte("test"),
		[]byte("test,foo,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		[]byte("foo"),
		[]byte("gohbase"),
	)
	if os, replaced := client.regions.put(region2); !replaced {
		t.Errorf("Expected to put new region into cache, got: %v", os)
	} else if len(os) != 0 {
		t.Errorf("Didn't expect any overlaps, got: %v", os)
	}
	region2.SetClient(regClient)
	client.clients.put("regionserver:1", region2, newClientFn)

	region3 := region.NewInfo(
		3,
		nil,
		[]byte("test"),
		[]byte("test,gohbase,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		[]byte("gohbase"),
		[]byte(""),
	)
	if os, replaced := client.regions.put(region3); !replaced {
		t.Errorf("Expected to put new region into cache, got: %v", os)
	} else if len(os) != 0 {
		t.Errorf("Didn't expect any overlaps, got: %v", os)
	}
	region3.SetClient(regClient)
	client.clients.put("regionserver:1", region3, newClientFn)

	_, err := DebugState(client)

	if err != nil {
		t.Errorf("DebugInfo should not have an error: %v", err)
	}
}
