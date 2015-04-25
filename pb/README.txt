These are the protobuf definition files used by GoHBase.
They were copied from HBase (see under hbase-protocol/src/main/protobuf).

The following changes were made to those files:
  - the package name was changed to "pb".
  - code is optimize_for LITE_RUNTIME instead of SPEED to reduce code bloat.

The files in this directory are also subject to the Apache License 2.0 and
are copyright of the Apache Software Foundation.
