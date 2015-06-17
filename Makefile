# Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
# This file is part of GoHBase.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

GO := go
GOTEST_FLAGS :=

DEFAULT_GOPATH := $${GOPATH%%:*}
GOPATH_BIN := $(DEFAULT_GOPATH)/bin
GOLINT := $(GOPATH_BIN)/golint

all: install

install:
	$(GO) install ./...

check: test fmtcheck lint

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

lint:
	find ./* -type d ! -name pb | xargs -L 1 $(GOLINT) &>lint; :
	if test -s lint; then echo Check these packages for golint:; cat lint; rm lint; exit 1; else rm lint; fi
# The above is ugly, but unfortunately golint doesn't exit 1 when it finds
# lint.  See https://github.com/golang/lint/issues/65

test:
	$(GO) test $(GOTEST_FLAGS) ./...

goproto:
	go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
	cd pb && protoc --go_out=. *.proto

gogoproto:
	go get github.com/gogo/protobuf/protoc-gen-gofast
	cd pb && protoc --gofast_out=. *.proto

.PHONY: all check coverage coverdata fmtcheck install lint test goproto gogoproto
