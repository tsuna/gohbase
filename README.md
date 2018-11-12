# Golang HBase client [![Build Status](https://travis-ci.org/reborn-go/gohbase.svg?branch=master)](https://travis-ci.org/reborn-go/gohbase) [![codecov.io](http://codecov.io/github/reborn-go/gohbase/coverage.svg?branch=master)](http://codecov.io/github/reborn-go/gohbase?branch=master) [![GoDoc](https://godoc.org/github.com/reborn-go/gohbase?status.png)](https://godoc.org/github.com/reborn-go/gohbase)

This is a pure [Go](http://golang.org/) client for [HBase](http://hbase.org).

Current status: beta.

## Supported Versions

HBase >= 1.0

## Installation

    go get github.com/reborn-go/gohbase

## Example Usage

#### Create a client
```go
client := gohbase.NewClient("localhost")
```
#### Insert a cell
```go
// Values maps a ColumnFamily -> Qualifiers -> Values.
values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte{0}}}
putRequest, err := hrpc.NewPutStr(context.Background(), "table", "key", values)
rsp, err := client.Put(putRequest)
```

#### Get an entire row
```go
getRequest, err := hrpc.NewGetStr(context.Background(), "table", "row")
getRsp, err := client.Get(getRequest)
```

#### Get a specific cell
```go
// Perform a get for the cell with key "15", column family "cf" and qualifier "a"
family := map[string][]string{"cf": []string{"a"}}
getRequest, err := hrpc.NewGetStr(context.Background(), "table", "15",
    hrpc.Families(family))
getRsp, err := client.Get(getRequest)
```

#### Get a specific cell with a filter
```go
pFilter := filter.NewKeyOnlyFilter(true)
family := map[string][]string{"cf": []string{"a"}}
getRequest, err := hrpc.NewGetStr(context.Background(), "table", "15",
    hrpc.Families(family), hrpc.Filters(pFilter))
getRsp, err := client.Get(getRequest)
```

#### Scan with a filter
```go
pFilter := filter.NewPrefixFilter([]byte("7"))
scanRequest, err := hrpc.NewScanStr(context.Background(), "table",
		hrpc.Filters(pFilter))
scanRsp, err := client.Scan(scanRequest)
```

## Contributing

Any help would be appreciated. Please use Github pull requests
to send changes for review. Please sign the
[Contributor License Agreement](https://docs.google.com/spreadsheet/viewform?formkey=dFNiOFROLXJBbFBmMkQtb1hNMWhUUnc6MQ)
when you send your first change for review.  

## License

Copyright © 2015 The GoHBase Authors. All rights reserved. Use of this source code is governed by the Apache License 2.0 that can be found in the [COPYING](COPYING) file.
