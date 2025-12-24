BINARY_NAME=cndl

.PHONY: build test clean run

build:
	go build -o $(BINARY_NAME) ./cmd/cndl

test:
	go test -v ./internal/...

clean:
	go clean
	rm -f $(BINARY_NAME)
	rm -rf .cndl/

run: build
	./$(BINARY_NAME)