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

deb: build
	rm -rf .tmp
	mkdir -p .tmp/$(BINARY)
	mkdir -p .tmp/$(BINARY)/DEBIAN
	cp $(BASE)/debian/control .tmp/$(BINARY)/DEBIAN/
	sed -i "s/\[arch\]/$(ARCH)/" .tmp/$(BINARY)/DEBIAN/control
	sed -i "s/\[version\]/`$(BINARY) version | cut -f3 -d" "`/" .tmp/$(BINARY)/DEBIAN/control
	sed -i "s/\[size\]/`du -B1 $(GOBIN)/$(BINARY) | cut -f1`/" .tmp/$(BINARY)/DEBIAN/control
	sed -i "s/\[maintainer\]/$(MAINTAINER)/" .tmp/$(BINARY)/DEBIAN/control
	sed -i "s/\[email\]/$(EMAIL)/" .tmp/$(BINARY)/DEBIAN/control
	cat .tmp/$(BINARY)/DEBIAN/control
	mkdir -p .tmp/$(BINARY)/usr/bin
	cp $(GOBIN)/$(BINARY) .tmp/$(BINARY)/usr/bin/$(BINARY)
	mkdir -p .tmp/$(BINARY)/usr/share/doc/$(BINARY)
	cp $(BASE)/LICENCE .tmp/$(BINARY)/usr/share/doc/$(BINARY)/copyright
	cp $(BASE)/CHANGELOG .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog
	sed -i "s/\[version\]/`$(BINARY) version | cut -f3 -d" "`/" .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog
	sed -i "s/\[maintainer\]/$(MAINTAINER)/" .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog
	sed -i "s/\[email\]/$(EMAIL)/" .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog
	sed -i "s/\[date\]/`date -u '+%a, %d %b %Y %T %z'`/" .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog
	cat .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog
	gzip -9 -c .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog > .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog.Debian.gz
	rm -f .tmp/$(BINARY)/usr/share/doc/$(BINARY)/changelog

	mkdir -p var/deb/
	dpkg-deb --build .tmp/$(BINARY) var/deb/$(BINARY)-`$(BINARY) version | cut -f3 -d" "`.deb
