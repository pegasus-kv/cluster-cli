build:
	go mod tidy
	go mod verify
	go build -o pegasus-cluster-cli cmd/minos/main.go cmd/minos/minos.go
