package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func callAPI(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

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

func getValueAtPath(path []string, input map[string]interface{}) interface{} {
	if check, ok := input[path[0]]; !ok || check == nil {
		return nil
	}
	if len(path) == 1 {
		return input[path[0]]
	}

	key := path[0]
	path = path[1:]

	nextInput, _ := input[key].(map[string]interface{})

	return getValueAtPath(path, nextInput)
}

func GenerateRecords(c util.Config) []interface{} {
	var responseMap map[string]interface{}

	apiResponse, err := callAPI(c.URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error calling API: %v\n", err)
		os.Exit(1)
	}

	output := string(apiResponse)
	responseMapRecordsPath := c.ResponseRecordsPath
	if len(c.ResponseRecordsPath) == 0 && output[0:1] == "{" {
		output = "{\"results\":[" + output + "]}"
		responseMapRecordsPath = []string{"results"}
	} else if len(c.ResponseRecordsPath) == 0 && output[0:1] == "[" {
		output = "{\"results\":" + output + "}"
		responseMapRecordsPath = []string{"results"}
	}

	json.Unmarshal([]byte(output), &responseMap)

	records, ok := getValueAtPath(responseMapRecordsPath, responseMap).([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	// PAGINATED, "next"
	if c.Paginated && c.PaginationStrategy == "next" {
		nextURL := getValueAtPath(c.PaginationNextPath, responseMap)
		if nextURL == nil {
			generateSurrogateKey(c, records)
			return records
		}

		nextConfig := c
		nextConfig.URL = nextURL.(string)
		records = append(records, GenerateRecords(nextConfig)...)
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
				Stream:        c.URL + "__" + strings.Join(c.ResponseRecordsPath, "__"),
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
