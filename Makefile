SHELL := /bin/bash

.PHONY: build test test-race lint fmt run

build:
	go build -o bin/alert-engine ./cmd/alert-engine

test:
	go test ./... -count=1

test-race:
	go test ./... -race -count=1

fmt:
	gofmt -w .

lint:
	golangci-lint run ./...

run:
	go run ./cmd/alert-engine