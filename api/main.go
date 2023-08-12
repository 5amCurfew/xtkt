package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("pong! 🏓")
		return
	})
	http.HandleFunc("/post", handlePost)
	fmt.Println("Server started on :8888")
	http.ListenAndServe(":8888", nil)
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	fmt.Fprintf(w, "Received a POST request")
}
