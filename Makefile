# lum build automation.
#
# Requirements: Go ≥1.26 and Rust (stable). That's it — protobuf codegen
# uses buf/protox (no protoc install), the vector store and embedding
# model are embedded/auto-downloaded (no Docker, no services).

TOOLS_BIN := $(CURDIR)/.tools/bin
export PATH := $(TOOLS_BIN):$(PATH)

.PHONY: all build build-go build-rust proto test test-go test-rust run clean

all: build

## build: compile both planes into ./bin (lum + lumen side by side,
## which is how lum auto-discovers the lumen binary).
build: build-rust build-go

build-go:
	mkdir -p bin
	cd control-plane && go build -o ../bin/lum ./cmd/lum

build-rust:
	mkdir -p bin
	cd data-plane && cargo build --release
	cp data-plane/target/release/lumen bin/lumen

## proto: regenerate Go code from proto/ (output is committed, so this
## is only needed after editing .proto files). Rust regenerates
## automatically via data-plane/build.rs.
proto:
	GOBIN=$(TOOLS_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	GOBIN=$(TOOLS_BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go run github.com/bufbuild/buf/cmd/buf@v1.65.0 generate

test: test-go test-rust

test-go:
	cd control-plane && go test ./...

test-rust:
	cd data-plane && cargo test

## run: build and start the daemon.
run: build
	./bin/lum serve

clean:
	rm -rf bin .tools
	cd data-plane && cargo clean
