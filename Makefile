build:
	rm -f *_catalog.json *_state.json *_schema.json
	go mod tidy
	go mod vendor
	go build .
