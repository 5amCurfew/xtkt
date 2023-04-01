package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func generateSurrogateKey(c Config, records []interface{}) {
	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			data := c.Unique_key + r[c.Primary_bookmark].(string)
			h := sha256.New()
			h.Write([]byte(data))

			hashBytes := h.Sum(nil)

			r["surrogate_key"] = hex.EncodeToString(hashBytes)
		}
	}
}

func GenerateRecords(c Config) []interface{} {
	var responseMap map[string]interface{}

	apiResponse, err := http.Get(c.Url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error calling API: %v\n", err)
		os.Exit(1)
	}

	defer apiResponse.Body.Close()

	body, err := io.ReadAll(apiResponse.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
	}

	output := string(body)

	if c.Response_records_path == "" && output[0:1] == "{" {
		output = "{\"results\":[" + output + "]}"
	} else if c.Response_records_path == "" && output[0:1] == "[" {
		output = "{\"results\":" + output + "}"
	}

	json.Unmarshal([]byte(output), &responseMap)

	records, ok := responseMap["results"].([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	generateSurrogateKey(c, records)

	return records

}

func GenerateRecordMessages(records []interface{}, c Config) {
	var bookmark string
	if c.Bookmark && c.Primary_bookmark != "" {
		bookmark = ReadBookmark(c)
	} else {
		bookmark = ""
	}

	for _, record := range records {
		r, _ := record.(map[string]interface{})

		if r[c.Primary_bookmark].(string) > bookmark {
			message := Message{
				Type:          "RECORD",
				Data:          r,
				Stream:        c.Url + "__" + c.Response_records_path,
				TimeExtracted: time.Now(),
			}

			messageJson, err := json.Marshal(message)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating RECORD message: %v\n", err)
				os.Exit(1)
			}

			fmt.Println(string(messageJson))
		}
	}
}
