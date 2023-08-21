clean:
	rm -f state.json history.json schema_*
	go mod tidy
	go mod vendor
	go build .
