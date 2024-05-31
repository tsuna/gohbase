package gohbase

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/region"
)

func TestDebugStateSanity(t *testing.T) {
	client := newClient("~invalid.quorum~") // We shouldn't connect to ZK.

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
		nil,
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

	jsonVal, err := DebugState(client)

	if err != nil {
		t.Fatalf("DebugInfo should not have an error: %v", err)
	}

	var jsonUnMarshal map[string]interface{}
	err = json.Unmarshal(jsonVal, &jsonUnMarshal)

	if err != nil {
		t.Fatalf("Encoutered eror when Unmarshalling: %v", err)
	}

	clientRegionMap := jsonUnMarshal["ClientRegionMap"]
	clientType := jsonUnMarshal["ClientType"]
	regionInfoMap := jsonUnMarshal["RegionInfoMap"]
	keyRegionCache := jsonUnMarshal["KeyRegionCache"]
	clientRegionCache := jsonUnMarshal["ClientRegionCache"]

	expectedClientRegionSize := 1
	regionInfoMapSize := 3

	assert.Equal(t, clientType.(string), string(region.RegionClient))
	assert.Equal(t, expectedClientRegionSize, len(clientRegionMap.(map[string]interface{})))
	assert.Equal(t, regionInfoMapSize, len(regionInfoMap.(map[string]interface{})))
	assert.Equal(t, 3, len(keyRegionCache.(map[string]interface{})))
	assert.Equal(t, len(clientRegionCache.(map[string]interface{})), 1) // only have one client

	assert.Equal(t, true, json.Valid(jsonVal))

}
