# Golib

A collection of Hafslund Go libraries.

## Available Packages

- **[telemetry](./telemetry)** - OpenTelemetry integration providing logging, metrics, and tracing capabilities
- **[identity](./identity)** - Support for fetching OAuth tokens based on K8s service account identity

## Development

This repository uses a Makefile to run common tasks across all modules:

- `make all` - Run linting, vetting, and tests (default target)
- `make test` - Run tests for all modules
- `make test-coverage` - Run tests with coverage reports for all modules
- `make vet` - Run go vet for all modules
- `make lint` - Run golangci-lint for all modules
- `make fmt` - Format code using golangci-lint for all modules

