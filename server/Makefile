SRCDIR=.
SRCFILES:=$(shell find $(SRCDIR)/.. -name '*.go')

BINARY=localproxy

VERSION=`git describe`
LDFLAGS=-ldflags "-X main.version=${VERSION}"

$(BINARY): $(SRCFILES)
	go build ${LDFLAGS} -o ${BINARY} server.go

all:
	go build ${LDFLAGS} .

.PHONY: install
install:
	go install ${LDFLAGS} ./...

.PHONY: clean
clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
