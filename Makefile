
unit-test:
	go test -v -cover -race ./...

lint:
	golangci-lint run --fix
	gofumpt -w .
