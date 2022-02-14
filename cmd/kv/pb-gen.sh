#!/bin/bash
cd $(dirname $0)
protoc --proto_path=../../pb/ --proto_path=pb/ --go_out=pb/ --go_opt=paths=source_relative \
    $(find . -iname "*.proto")
