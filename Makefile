
GO ?= go
BIN_DIR ?= bin

.PHONY: unit-test
unit-test:
	go test -v -cover -race ./...

.PHONY: coverage-test
coverage-test:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	@echo ""
	@echo "Coverage by function:"
	@go tool cover -func=coverage.out | tail -20
	@echo ""
	@go tool cover -func=coverage.out | grep total

.PHONY: e2e-test
e2e-test:
	go test -tags=e2e ./test/e2e -v

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
