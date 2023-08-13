package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
)

type xtktRequest struct {
	Config string `json:"config"`
}

func main() {
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("pong! 🏓")
		return
	})
	http.HandleFunc("/post", handlePost)
	fmt.Println("Server started on :8888")
	http.ListenAndServe(":8888", nil)
}

// curl -X POST -H "Content-Type: application/json" -d '{"config": "hello"}' http://localhost:3000/post
func handlePost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse JSON body
	var req xtktRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Error parsing JSON", http.StatusBadRequest)
		return
	}

	fmt.Println(req)

	cmd := exec.Command("xtkt", "--version")

	var outBuffer, errBuffer bytes.Buffer
	cmd.Stdout = &outBuffer
	cmd.Stderr = &errBuffer

	err = cmd.Run()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing command: %s", err), http.StatusInternalServerError)
		return
	}
	fmt.Print(outBuffer.String())
	fmt.Print(errBuffer.String())
}
