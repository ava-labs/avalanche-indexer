
GO ?= go
BIN_DIR ?= bin

.PHONY: unit-test
unit-test:
	go test -v -cover -race ./...

# Packages to exclude from coverage (entry points, test utilities)
COVER_EXCLUDE := /cmd/|/testutils

.PHONY: coverage-test
coverage-test:
	@COVERPKGS=$$(go list ./... | grep -vE '$(COVER_EXCLUDE)' | tr '\n' ',' | sed 's/,$$//'); \
	go test -race -coverprofile=coverage.out -covermode=atomic -coverpkg="$$COVERPKGS" ./...
	@echo ""
	@echo "Coverage by function:"
	@go tool cover -func=coverage.out | tail -20
	@echo ""
	@go tool cover -func=coverage.out | grep total

.PHONY: e2e-test
e2e-test:
	go test -tags=e2e ./test/e2e -v

# Fuzz testing - short duration for CI (30s per target)
.PHONY: fuzz-test
fuzz-test:
	@echo "Running fuzz tests (30s each)..."
	go test -fuzz=FuzzHexToBytes32 -fuzztime=30s ./pkg/utils/ -v
	go test -fuzz=FuzzHexToBytes20 -fuzztime=30s ./pkg/utils/ -v
	go test -fuzz=FuzzHexToBytes8 -fuzztime=30s ./pkg/utils/ -v
	go test -fuzz=FuzzCorethBlockUnmarshal -fuzztime=30s ./pkg/kafka/messages/ -v
	go test -fuzz=FuzzCorethTransactionUnmarshal -fuzztime=30s ./pkg/kafka/messages/ -v
	go test -fuzz=FuzzCorethWithdrawalUnmarshal -fuzztime=30s ./pkg/kafka/messages/ -v
	go test -fuzz=FuzzCorethBlockMarshalRoundtrip -fuzztime=30s ./pkg/kafka/messages/ -v

# Fuzz testing - extended duration for scheduled runs (5m per target)
.PHONY: fuzz-test-long
fuzz-test-long:
	@echo "Running extended fuzz tests (5m each)..."
	go test -fuzz=FuzzHexToBytes32 -fuzztime=5m ./pkg/utils/ -v
	go test -fuzz=FuzzHexToBytes20 -fuzztime=5m ./pkg/utils/ -v
	go test -fuzz=FuzzHexToBytes8 -fuzztime=5m ./pkg/utils/ -v
	go test -fuzz=FuzzCorethBlockUnmarshal -fuzztime=5m ./pkg/kafka/messages/ -v
	go test -fuzz=FuzzCorethTransactionUnmarshal -fuzztime=5m ./pkg/kafka/messages/ -v
	go test -fuzz=FuzzCorethWithdrawalUnmarshal -fuzztime=5m ./pkg/kafka/messages/ -v
	go test -fuzz=FuzzCorethBlockMarshalRoundtrip -fuzztime=5m ./pkg/kafka/messages/ -v

.PHONY: lint
lint:
	golangci-lint run --fix
	gofumpt -w .

.PHONY: build-all
build-all:
	@mkdir -p $(BIN_DIR)
	@# Only build directories in cmd/ that contain main.go (excludes cmd/utils, etc.)
	@set -e; \
	for d in cmd/*; do \
		if [ -d "$$d" ] && [ -f "$$d/main.go" ]; then \
			name=$$(basename "$$d"); \
			echo "Building $$name..."; \
			$(GO) build -o $(BIN_DIR)/$$name ./$$d || exit 1; \
		fi; \
	done

.PHONY: build-app
build-app:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$(APP) ./cmd/$(APP)

.PHONY: clean
clean:
	@echo "Cleaning $(BIN_DIR)..."
	rm -rf $(BIN_DIR)
