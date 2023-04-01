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

	util "github.com/5amCurfew/xtkt/util"
)

func generateSurrogateKey(c util.Config, records []interface{}) {
	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			data := c.UniqueKey + r[c.PrimaryBookmark].(string)
			h := sha256.New()
			h.Write([]byte(data))

			hashBytes := h.Sum(nil)

			r["surrogate_key"] = hex.EncodeToString(hashBytes)
		}
	}
}

func GenerateRecords(c util.Config) []interface{} {
	var responseMap map[string]interface{}

	apiResponse, err := http.Get(c.URL)
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
	responseMapRecordsPath := c.ResponseRecordsPath
	if c.ResponseRecordsPath == "" && output[0:1] == "{" {
		output = "{\"results\":[" + output + "]}"
		responseMapRecordsPath = "results"
	} else if c.ResponseRecordsPath == "" && output[0:1] == "[" {
		output = "{\"results\":" + output + "}"
		responseMapRecordsPath = "results"
	}

	json.Unmarshal([]byte(output), &responseMap)

	records, ok := responseMap[responseMapRecordsPath].([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	generateSurrogateKey(c, records)

	return records

}

func GenerateRecordMessages(records []interface{}, c util.Config) {
	var bookmark string
	if c.Bookmark && c.PrimaryBookmark != "" {
		bookmark = readBookmark(c)
	} else {
		bookmark = ""
	}

	for _, record := range records {
		r, _ := record.(map[string]interface{})

		if r[c.PrimaryBookmark].(string) > bookmark {
			message := util.Message{
				Type:          "RECORD",
				Data:          r,
				Stream:        c.URL + "__" + c.ResponseRecordsPath,
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
