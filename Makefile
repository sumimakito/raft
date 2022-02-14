GO = go
BINDIR = bin

.PHONY: all clean dep kv pb pbclean test testcov vet

all: dep pb testcov kv

clean: pbclean
	$(GO) clean
	rm -f $(BINDIR)/kv

dep:
	$(GO) mod download

kv:
	$(GO) build -o $(BINDIR)/kv -v ./cmd/kv/...

pb:
	protoc --proto_path=pb/ --go_out=pb/ --go_opt=paths=source_relative \
		--go-grpc_out=pb/ --go-grpc_opt=paths=source_relative \
		$(shell find pb -iname "*.proto")
	protoc --proto_path=pb/ --proto_path=cmd/kv/pb/ \
		--go_out=cmd/kv/pb/ --go_opt=paths=source_relative \
		--go-grpc_out=cmd/kv/pb/ --go-grpc_opt=paths=source_relative \
		$(shell find cmd/kv/pb -iname "*.proto")

pbclean:
	find . -iname "*.pb.go" -type f -delete

test:
	$(GO) test -v ./...

testcov:
	$(GO) test -v -coverpkg=./... -coverprofile=__test.cov ./...

vet:
	$(GO) vet