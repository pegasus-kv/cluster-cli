build:
	go mod verify && go mod tidy
	go build -o bin/pegasus-cluster-cli cmd/minos/main.go cmd/minos/minos.go
