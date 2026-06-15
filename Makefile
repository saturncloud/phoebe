BINARY := phoebe
PKG := ./cmd/interceptor
GOLANGCI_LINT_VERSION := v1.64.8

.PHONY: build
build:
	go build -o bin/$(BINARY) $(PKG)

.PHONY: run
run: build
	./bin/$(BINARY) -f config/settings.example.yaml

.PHONY: test
test:
	go test ./...

# Runs the live-Postgres conformance tests (the `integration` build tag). These
# prove the production SQL rater computes the same money as the Rate() oracle —
# including the sum-then-round behavior a unit test can't exercise. Requires
# PHOEBE_TEST_DATABASE_URL pointing at a Postgres with btree_gist available.
.PHONY: integration-test
integration-test:
	go test -tags=integration ./...

.PHONY: vet
vet:
	go vet ./...

# Installs the pinned golangci-lint into GOPATH/bin if absent, then runs it.
.PHONY: lint
lint:
	@command -v golangci-lint >/dev/null 2>&1 || \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	golangci-lint run

.PHONY: fmt
fmt:
	gofmt -l -w .

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: clean
clean:
	rm -rf bin
