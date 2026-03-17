.PHONY: run build test migrate up down

up:
	docker-compose up -d

down:
	docker-compose down

build:
	go build -o broker ./cmd/broker/main.go

run: up
	export $(shell cat .env | xargs) && go run ./cmd/broker/main.go

test:
	TEST_POSTGRES_URL=postgres://appuser:apppassword@localhost:5432/appdb?sslmode=disable \
	TEST_REDIS_ADDR=localhost:6379 \
	go test ./...

lint:
	go vet ./...

docker-build:
	docker build -t mini-kafka .