install_binaries:
	go install github.com/golang/mock/mockgen@v1.6.0

generate_mocks:
	go generate ./...

initialize: # install_binaries generate_mocks
	@echo hi friends

test:
	@go test ./...