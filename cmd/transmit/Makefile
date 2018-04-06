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

DEPSLIST =

deps:
	$(GOGET) github.com/juju/ratelimit
	$(GOGET) github.com/midbel/cli
	$(GOGET) github.com/midbel/rustine
	$(GOGET) github.com/midbel/rustine/rw
	$(GOGET) github.com/midbel/toml
	$(GOGET) github.com/midbel/transmit

clean:
	rm -f $(GOBIN)/$(BINARY)
