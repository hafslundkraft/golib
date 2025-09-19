all: lint vet test

MODULES=$(shell go list -f '{{.Dir}}' -m)

test: $(MODULES)
	cd $^ && go test ./...

lint: $(MODULES)
	cd $^ && golangci-lint run --config=$(PWD)/.golangci.toml ./...

vet: $(MODULES)
	cd $^ && go vet ./...
	find -type f -name go.mod -printf '%h\n' -execdir go vet ./... \;

fmt: $(MODULES)
	cd $^ && golangci-lint fmt --config=$(PWD)/.golangci.toml ./...

.PHONY: test lint vet fmt
