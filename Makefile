clear:
	rm -f state.json schema_*
	go mod tidy
	go mod vendor
	go build .
