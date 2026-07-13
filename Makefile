.PHONY: build test test-cover lint clean proto

build:
	go build ./...

test:
	go test -race -count=1 ./...

test-cover:
	go test -race -coverprofile=coverage.out ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/ coverage.out

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	    proto/raft/raft.proto \
	    proto/storage/storage.proto \
	    proto/client/client.proto
