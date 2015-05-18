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

COVER_PKGS := `find ./* -name '*_test.go' | xargs -I{} dirname {} | sort -u`
COVER_MODE := count
coverdata:
	echo 'mode: $(COVER_MODE)' >coverage.out
	for dir in $(COVER_PKGS); do \
	  $(GO) test -covermode=$(COVER_MODE) -coverprofile=cov.out-t $$dir || exit; \
	  tail -n +2 cov.out-t >> coverage.out && \
	  rm cov.out-t; \
	done;

coverage: coverdata
	$(GO) tool cover -html=coverage.out
	rm -f coverage.out

fmtcheck:
	errors=`gofmt -l .`; if test -n "$$errors"; then echo Check these files for style errors:; echo "$$errors"; exit 1; fi

test:
	$(GO) test $(GOTEST_FLAGS) ./...

.PHONY: all check coverage coverdata fmtcheck install test
