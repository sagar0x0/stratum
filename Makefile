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
