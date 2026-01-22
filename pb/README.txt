These are the protobuf definition files used by GoHBase.
They were copied from HBase (see under hbase-protocol/src/main/protobuf).

Currently using .proto files from HBase branch-1.3, with unnecessary .proto
files removed.

The following changes were made to those files:
  - the package name was changed to "pb". (`sed -i 's/hbase.pb/pb/g' ./*`)

The files in this directory are also subject to the Apache License 2.0 and
are copyright of the Apache Software Foundation.

To add/update a *.proto file as of hbase 2.6.2:
1. Copy *.proto file from hbase source code in hbase-protocol-shaded/src/main/protobuf to ./pb
2. Update copied file to contain
```
package pb;

option java_package = "org.apache.hadoop.hbase.protobuf.generated";

option go_package = "../pb";
```
3. Update `pb/gen.go` to list any new *.proto file
4. Run `cd ./pb && go generate` to generate new *.pb.go files
