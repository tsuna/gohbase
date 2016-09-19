// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package mock_test

import (
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/test/mock"
)

var _ gohbase.Client = (*mock.MockClient)(nil)
