GO = go
PROTOC = protoc
BINDIR = bin

.PHONY: all ci clean dep htmlcov kv pb pbclean test testcov vet

all: dep pb testcov kv

ci: dep pb testcov

clean: pbclean
	$(GO) clean
	rm -f $(BINDIR)/kv

dep:
	$(GO) mod download -x

htmlcov: testcov
	$(GO) tool cover -html=coverage.out

kv:
	$(GO) build -o $(BINDIR)/kv -v ./cmd/kv

pb:
	$(PROTOC) --proto_path=pb/ --go_out=pb/ --go_opt=paths=source_relative \
		--go-grpc_out=pb/ --go-grpc_opt=paths=source_relative \
		$(shell find pb -iname "*.proto")
	$(PROTOC) --proto_path=pb/ --proto_path=cmd/kv/pb/ \
		--go_out=cmd/kv/pb/ --go_opt=paths=source_relative \
		--go-grpc_out=cmd/kv/pb/ --go-grpc_opt=paths=source_relative \
		$(shell find cmd/kv/pb -iname "*.proto")

pbclean:
	find . -iname "*.pb.go" -type f -delete

test:
	$(GO) test -v  ./...

testcov:
	$(GO) test -v -race -covermode=atomic -coverprofile=coverage.out ./...

vet:
	$(GO) vet ./...
