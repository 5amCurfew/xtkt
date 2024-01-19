build:
	rm -f state_* schema_*
	go mod tidy
	go mod vendor
	go build .
