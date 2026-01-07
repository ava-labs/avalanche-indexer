
GO ?= go
BIN_DIR ?= bin

.PHONY: unit-test
unit-test:
	go test -v -cover -race ./...

.PHONY: lint
lint:
	golangci-lint run --fix
	gofumpt -w .

.PHONY: build-all
build-all:
	@mkdir -p $(BIN_DIR)
	@set -e; \
	for d in cmd/*; do \
		if [ -d "$$d" ]; then \
			name=$$(basename "$$d"); \
			echo "Building $$name..."; \
			$(GO) build -o $(BIN_DIR)/$$name ./$$d; \
		fi; \
	done

.PHONY: build-ap
build-app:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$(APP) ./cmd/$(APP)

.PHONY: clean
clean:
	@echo "Cleaning $(BIN_DIR)..."
	rm -rf $(BIN_DIR)
