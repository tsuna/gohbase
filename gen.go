// Copyright (c) 2015 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
// Subject to Arista Networks, Inc.'s EULA.
// FOR INTERNAL USE ONLY. NOT FOR DISTRIBUTION.

package gohbase

// to run this command, gomock and mockgen needs to be installed, by running
// go get github.com/golang/mock/gomock
// go get github.com/golang/mock/mockgen
// then run this command to auto generate mock_client

//go:generate mockgen -source ./client.go -destination ./test/mock/client.go -package mock
