go_version := "1.17"

.PHONY: all
all: build test lint

.PHONY: build
build:
#   Strip debug symbols: -ldflags "-w"
	go build -o bin/sophiadb -race -ldflags "-w" cmd/sophiadb.go

.PHONY: test
test: gen
	go test -race -shuffle on -coverprofile=coverage.out -covermode atomic ./...
	@go tool cover -func coverage.out | grep -E "^total:.+?\d+.\d+%"

.PHONY: test_ci
test_ci:
	go test -race -shuffle on -coverprofile=coverage.out -covermode atomic -count=1 -v ./...
	go tool cover -func coverage.out

.PHONY: vendor
vendor:
	go mod vendor -v

.PHONY: lint
lint:
	golangci-lint run ./... --timeout=120s --max-same-issues=0 --sort-results --go=$(go_version)

.PHONY: lint_ci
lint_ci:
	golangci-lint run ./... --timeout=120s --max-same-issues=0 --sort-results --go=$(go_version) -v

.PHONY: upgrade
upgrade_deps:
	go get -u ./...
	go mod tidy
	go mod vendor

.PHONY: gen
gen:
	go generate ./...
