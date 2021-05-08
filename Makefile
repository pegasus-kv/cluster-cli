build:
	go mod verify && go mod tidy
	go build -o bin/minos cmd/minos/*.go
