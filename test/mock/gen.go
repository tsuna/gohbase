// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package mock

// To run this command, mockgen need to be installed, by running
//    go install go.uber.org/mock/mockgen@v0.4.0
// then run 'go generate' to auto-generate mock_client.

//go:generate mockgen -destination=client.go -package=mock github.com/tsuna/gohbase Client
//go:generate mockgen -destination=admin_client.go -package=mock github.com/tsuna/gohbase AdminClient
//go:generate mockgen -destination=conn.go -package=mock net Conn
//go:generate mockgen -destination=call.go -package=mock github.com/tsuna/gohbase/hrpc Call
//go:generate mockgen -destination=zk/client.go -package=mock github.com/tsuna/gohbase/zk Client
//go:generate mockgen -destination=region/client.go -package=mock github.com/tsuna/gohbase/hrpc RegionClient
//go:generate mockgen -destination=rpcclient.go -package=mock github.com/tsuna/gohbase RPCClient
