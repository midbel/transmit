GOBIN=bin
GOCMD=go
GOBUILD=$(GOCMD) build
GOGET=$(GOCMD) get

SOURCE=src/github.com/midbel/transmit/cmd/transmit/*go
BINARY=transmit
VERSION=

LDFLAGS = "-s -w -X github.com/midbel/cli.Version=$(VERSION) -X github.com/midbel/cli.BuildTime=`date -u +%FT%T`"

.PHONY: clean

build:
	. ./envvars
	$(GOBUILD) -ldflags $(LDFLAGS) -o $(GOBIN)/$(BINARY) $(SOURCE)

DEPSLIST = \
	github.com/juju/ratelimit \
	github.com/midbel/cli \
	github.com/midbel/rustine \
	github.com/midbel/rustine/rw \
	github.com/midbel/toml \
	github.com/midbel/transmit \

deps:
	for i in $(DEPSLIST); do \
		$(GOGET) $$i; \
	done

clean:
	rm -f $(GOBIN)/$(BINARY)
