clean:
	rm -f state.json history.json
	go mod tidy
	go build .