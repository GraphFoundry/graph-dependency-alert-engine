SHELL := /bin/bash

.PHONY: build test test-race lint fmt

build:
	go build ./...

test:
	go test ./... -count=1

test-race:
	go test ./... -race -count=1

fmt:
	gofmt -w .

lint:
	golangci-lint run ./...
