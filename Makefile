all: lint vet test

test:
	find -type f -name go.mod -printf '%h\n' -execdir go test ./... \;

lint:
	find -type f -name go.mod -printf '%h\n' -execdir golangci-lint run --config=$(PWD)/.golangci.toml ./... \;

vet:
	find -type f -name go.mod -printf '%h\n' -execdir go vet ./... \;

fmt:
	find -type f -name go.mod -printf '%h\n' -execdir golangci-lint fmt --config=$(PWD)/.golangci.toml ./... \;

.PHONY: test lint vet fmt
