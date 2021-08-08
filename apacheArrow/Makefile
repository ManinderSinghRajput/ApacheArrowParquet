# set default shell
SHELL = bash -e -o pipefail

# Variables
SERVER_NAME = main.go
CLIENT_NAME = ../client/client.go
SERVER_BIN_NAME = server
CLIENT_BIN_NAME = client
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
	@${GO} build -o build/${SERVER_BIN_NAME} ${SERVER_NAME}
	@${GO} build -o build/${CLIENT_BIN_NAME} ${CLIENT_NAME}

run: build
	@./${SERVER_BIN_NAME} ${ARGS}

test:
	@${GO} test -cover ${PACKAGES}

lint:
	@${GO_LINTER} run

clean:
	@rm -rf build/*

prep: fmt lint build test
