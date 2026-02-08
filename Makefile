.PHONY: proto build run clean test deps fmt lint docker-up docker-down example

build:
	go build -o bin/comet ./cmd/comet

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/comet/v1/broker.proto

run: build
	./bin/comet

clean:
	rm -rf bin/
	rm -rf data/

test:
	go test -v -race ./...

deps:
	go mod download
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

fmt:
	go fmt ./...

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

example:
	@trap 'kill 0' EXIT; \
	go run ./examples/price-producer & \
	sleep 2; \
	go run ./examples/price-alert & \
	wait
