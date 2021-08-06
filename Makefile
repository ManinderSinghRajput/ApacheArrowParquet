# set default shell
SHELL = bash -e -o pipefail

# Variables
SERVER_NAME = main.go
CLIENT_NAME = client/client.go
SERVER_BIN_NAME = server
CLIENT_BIN_NAME = client
GO_LINTER = golangci-lint
ARGS = "/tmp/small.csv"
DEBUG_ARGS = -debug
# Packages
PACKAGES = $(shell go list ./...)

GO = go

fmt:
	@${GO} fmt ${PACKAGES}

mod-update:
	@${GO} mod tidy

mod-download:
	@${GO} mod download

mod-vendor: mod-update
	@${GO} mod vendor

build: clean
	@if test ! -d build; then mkdir build; fi
	@${GO} build -o build/${SERVER_BIN_NAME} ${SERVER_NAME}
	@${GO} build -o build/${CLIENT_BIN_NAME} ${CLIENT_NAME}

run: build
	@./${SERVER_BIN_NAME} ${ARGS}

clean-proto:
	rm -rf go/*

proto: clean-proto
	protoc --go_out=plugins=grpc:. protos/service.proto

test:
	@${GO} test -cover ${PACKAGES}

lint:
	@${GO_LINTER} run

clean:
	@rm -rf build/*

prep: fmt mod-update mod-download lint build test
