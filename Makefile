proto-compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.

run :
	go run ./...

test:
	go test -race ./...
