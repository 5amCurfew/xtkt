package lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// curl -X POST -H "Content-Type: application/json" -d '{"key1":"value1","key2":"value2"}' http://localhost:8080/records

func StartListening(config Config) {
	http.HandleFunc("/records", handleIncomingRecords(config))
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			fmt.Println("Server error:", err)
			os.Exit(1)
		}
	}()
	log.Info(fmt.Sprintf(`xtkt started listening on port 8080 at %s`, time.Now().UTC().Format(time.RFC3339)))

	// Keep the main goroutine running
	select {}
}

func handleIncomingRecords(config Config) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Content-Type") != "application/json" {
			log.Println("only Content-Type: application/json is supported")
			return
		}

		var record interface{}
		decoder := json.NewDecoder(req.Body)
		if err := decoder.Decode(&record); err != nil {
			// error parsing the JSON, return the original output
			log.Info(fmt.Sprintf(`error JSON.UNMARSHAL REQUEST at %s, skipping`, time.Now().UTC().Format(time.RFC3339)))
			return
		}

		generateHashedRecordsFields(&record, config)
		generateSurrogateKey(&record, config)

		r, _ := record.(map[string]interface{})

		message := Message{
			Type:   "RECORD",
			Record: r,
			Stream: *config.StreamName,
		}

		messageJson, _ := json.Marshal(message)
		// os.Stdout.Write() different location when running as server?
		fmt.Println(string(messageJson))
	}
}
