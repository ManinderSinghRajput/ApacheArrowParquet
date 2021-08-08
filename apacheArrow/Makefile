# set default shell
SHELL = bash -e -o pipefail

# Variables
SERVER_NAME = main.go
CLIENT_NAME = ../client/client.go
SERVER_BIN_NAME = build/server
CLIENT_BIN_NAME = build/client
GO_LINTER = golangci-lint
ARGS = "output.arrow"
DEBUG_ARGS = -debug
# Packages
PACKAGES = $(shell go list ./...)

GO = go

fmt:
	@${GO} fmt ${PACKAGES}

build: clean
	@if test ! -d build; then mkdir build; fi
	@${GO} build -o ${SERVER_BIN_NAME} ${SERVER_NAME}
	@${GO} build -o ${CLIENT_BIN_NAME} ${CLIENT_NAME}

build-client: clean-client
	@${GO} build -o ${CLIENT_BIN_NAME} ${CLIENT_NAME}

mod-update:
	@${GO} mod tidy
	@${GO} mod vendor

mod-download:
	@${GO} mod download

mod-vendor: mod-update
	@${GO} mod vendor

run: build
	@${SERVER_BIN_NAME} ${ARGS}

run-client: build-client
	@${CLIENT_BIN_NAME}

produce:
	@${GO} run producer/producer.go

test:
	@${GO} test -cover ${PACKAGES}

lint:
	@${GO_LINTER} run

clean:
	@rm -rf build/*

clean-client:
	@rm -rf build/client

prep: fmt mod-update mod-download lint build test
