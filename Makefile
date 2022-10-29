install_binaries:
	go install github.com/golang/mock/mockgen@v1.6.0

generate_mocks:
	go generate ./...

initialize: install_binaries generate_mocks

clean:
	@rm -rf ./dist

test:
	@go test ./...

d: dev
i: initialize
c: clean

# watch / develop
dev_pipeline: test
watch:
	@watchexec -cr -f "*.go" -- make dev_pipeline
dev: watch
