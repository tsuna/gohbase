# Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
# This file is part of GoHBase.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

GO := go
GOTEST_FLAGS :=

all: install

install:
	$(GO) install ./...

check: test fmtcheck

fmtcheck:
	errors=`gofmt -l .`; if test -n "$$errors"; then echo Check these files for style errors:; echo "$$errors"; exit 1; fi

test:
	$(GO) test $(GOTEST_FLAGS) ./...

.PHONY: all check fmtcheck install test
