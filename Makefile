.PHONY: build test build-all build-armv7 build-aarch64 build-x86_64 clean

build:
	cargo build --release

test:
	cargo test

build-armv7:
	cargo zigbuild --release --target armv7-unknown-linux-musleabihf

build-aarch64:
	cargo zigbuild --release --target aarch64-unknown-linux-musl

build-x86_64:
	cargo zigbuild --release --target x86_64-unknown-linux-musl

build-all: build-armv7 build-aarch64 build-x86_64

clean:
	cargo clean
