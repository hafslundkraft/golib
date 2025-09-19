all: lint vet test

MODEXEC = find . -type f -name go.mod -execdir

test:
	$(MODEXEC) go test ./... \;

vet:
	$(MODEXEC) go vet ./... \;

lint:
	$(MODEXEC) golangci-lint run --config=$(PWD)/.golangci.toml ./... \;

fmt:
	$(MODEXEC) golangci-lint fmt --config=$(PWD)/.golangci.toml ./... \;

.PHONY: all test lint vet fmt
