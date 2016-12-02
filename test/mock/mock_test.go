// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package mock_test

import (
	"io"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/test/mock"
	regionMock "github.com/tsuna/gohbase/test/mock/region"
	zkMock "github.com/tsuna/gohbase/test/mock/zk"
	"github.com/tsuna/gohbase/zk"
)

var _ gohbase.Client = (*mock.MockClient)(nil)
var _ gohbase.AdminClient = (*mock.MockAdminClient)(nil)
var _ hrpc.Call = (*mock.MockCall)(nil)
var _ io.ReadWriteCloser = (*mock.MockReadWriteCloser)(nil)
var _ zk.Client = (*zkMock.MockClient)(nil)
var _ hrpc.RegionClient = (*regionMock.MockRegionClient)(nil)
