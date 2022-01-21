#!/bin/bash

build_protos() {
    protoc --proto_path=$1 --go_out=$1 --go_opt=paths=source_relative \
        --go-grpc_out=$1 --go-grpc_opt=paths=source_relative \
        $(find $1 -iname "*.proto")
}

build_protos pb
