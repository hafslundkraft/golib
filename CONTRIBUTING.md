# Contributing to Golib

Thank you for your interest in contributing to Golib! This document provides guidelines and instructions for contributing to this collection of Hafslund Go libraries.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/golib.git
   cd golib
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/hafslundkraft/golib.git
   ```

## Development Workflow

This repository uses a Makefile to run common development tasks:

- `make all` - Run all checks (lint, vet, test) - run this before submitting PRs
- `make test` - Run tests for all modules
- `make test-coverage` - Run tests with coverage reports
- `make vet` - Run go vet for static analysis
- `make lint` - Run golangci-lint for code quality checks
- `make fmt` - Format code according to project standards

### Prerequisites

- Go 1.25.1 or later
- golangci-lint (for linting)

## Code Standards

### Go Formatting

- All code must be formatted with `gofmt`
- Use `make fmt` to format code according to project standards
- Follow [Effective Go](https://golang.org/doc/effective_go) guidelines
- Follow [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments)

### Linting

- Code must pass `golangci-lint` checks without errors
- Run `make lint` before submitting a pull request
- Configuration is defined in `.golangci.toml`

### Code Structure

- Keep packages focused and single-purpose
- Export only what needs to be public
- Write clear, descriptive function and variable names
- Add package-level documentation

## Testing Requirements

### Writing Tests

- All new code should include tests
- Aim for meaningful test coverage (run `make test-coverage` to check)
- Test files should be named `*_test.go`
- Use table-driven tests where appropriate
- Use `testify` for assertions when needed

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run tests for a specific package
cd telemetry && go test ./...
```

## Pull Request Process

### Branch Naming

Use descriptive branch names with a prefix:
- `feature/` - New features
- `fix/` - Bug fixes
- `refactor/` - Code refactoring
- `docs/` - Documentation updates

Example: `feature/add-metrics-middleware`

### Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in the imperative mood (e.g., "Add", "Fix", "Update")
- Keep the subject line under 50 characters
- Add a detailed description if needed

Example:
```
Add HTTP metrics middleware

Implements middleware for tracking HTTP request metrics including
duration, status codes, and request counts.
```

### Before Submitting

1. Run `make all` to ensure all checks pass
2. Update documentation if needed
3. Add or update tests for your changes
4. Ensure your branch is up to date with `main`:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

### Submitting

1. Push your changes to your fork
2. Open a pull request against the `main` branch
3. Provide a clear description of the changes
4. Link any related issues
5. Wait for review and address any feedback

## Adding New Packages

When adding a new reusable package to this repository:

1. Create a new subdirectory with a descriptive name
2. Initialize with `go mod init github.com/hafslundkraft/golib/<package-name>`
3. Add a README.md in the package directory explaining:
   - Purpose and functionality
   - Installation instructions
   - Usage examples
   - API documentation
4. Update the main README.md to list the new package
5. Ensure the package works with the repository Makefile
6. Add comprehensive tests

### Package Structure

```
golib/
├── your-package/
│   ├── go.mod
│   ├── go.sum
│   ├── README.md
│   ├── *.go
│   └── *_test.go
```

## Documentation

### Code Documentation

- All exported functions, types, and constants must have documentation comments
- Package-level documentation should be in a `doc.go` file or at the top of the main package file
- Follow [Go Doc Comments](https://go.dev/doc/comment) guidelines
- Examples are encouraged using `Example` functions in test files

### README Updates

- Update the main README.md when adding new packages
- Keep package descriptions concise and clear
- Update the package's own README.md with detailed usage examples

## Code Review

All contributions require code review before merging. Reviewers will check:

- Code quality and adherence to Go best practices
- Test coverage and quality
- Documentation completeness
- Consistency with existing code style
- Performance implications

## Questions or Issues?

- Open an issue for bugs or feature requests
- Use discussions for questions about usage or design
- Be respectful and constructive in all interactions

## License

By contributing to Golib, you agree that your contributions will be licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
