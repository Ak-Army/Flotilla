all: flotilla-clients flotilla-servers

flotilla-clients:
	go build -o flotilla_client flotilla-client/main.go

flotilla-servers:
	go build -o flotilla_server flotilla-server/main.go
