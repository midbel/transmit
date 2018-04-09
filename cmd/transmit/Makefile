GOBIN=bin
GOCMD=go
GOBUILD=$(GOCMD) build
GOGET=$(GOCMD) get

BASE=src/github.com/midbel/transmit
SOURCE=$(BASE)/cmd/transmit/*go
BINARY=transmit
VERSION=
ARCH=amd64

LDFLAGS = "-s -w -X github.com/midbel/cli.Version=$(VERSION) -X github.com/midbel/cli.BuildTime=`date -u +%FT%T`"

.PHONY: clean

build:
	GOARCH=$(ARCH) $(GOBUILD) -ldflags $(LDFLAGS) -o $(GOBIN)/$(BINARY) $(SOURCE)

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
