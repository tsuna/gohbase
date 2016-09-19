// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package mock

// To run this command, gomock and mockgen need to be installed, by running
//    go get github.com/golang/mock/gomock
//    go get github.com/golang/mock/mockgen
// then run go generate to auto-generate mock_client.

//go:generate mockgen -package=mock -source=../../client.go -destination=client.go
