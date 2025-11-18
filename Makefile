all: lint vet test

MODEXEC = find . -type f -name go.mod -execdir

test:
	$(MODEXEC) go test ./... \;

test-coverage:
	$(MODEXEC) sh -c 'go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out' \;

vet:
	$(MODEXEC) go vet ./... \;

lint:
	$(MODEXEC) golangci-lint run --config=$(PWD)/.golangci.toml ./... \;

fmt:
	$(MODEXEC) golangci-lint fmt --config=$(PWD)/.golangci.toml ./... \;

.PHONY: all test test-coverage lint vet fmt
