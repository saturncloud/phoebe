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

# Race-detector gate for the concurrency-sensitive billing paths. `make test`
# does not enable -race (it would slow the whole suite), but proxy and waker
# decide whether a billable attempt is emitted exactly once under concurrent
# aborts, so a data race there is a money bug. Run this in CI alongside `test`.
.PHONY: race-test
race-test:
	go test -race -count=1 ./internal/proxy/... ./internal/waker/...

# Exercises admission Lua atomics against a real Valkey/Redis-compatible server.
# Example: PHOEBE_TEST_ADMISSION_VALKEY_ADDR=127.0.0.1:16379 make admission-integration-test
.PHONY: admission-integration-test
admission-integration-test:
	@test -n "$$PHOEBE_TEST_ADMISSION_VALKEY_ADDR" || \
		( echo "PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required"; exit 1 )
	go test -tags=admissionintegration ./internal/admission -run TestRealValkeyAtomicAdmission -count=1

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
