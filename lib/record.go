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

func callAPI(config util.Config) ([]byte, error) {
	req, _ := http.NewRequest("GET", config.URL, nil)

	if config.AuthStrategy == "basic" && config.AuthUsername != "" && config.AuthPassword != "" {
		req.SetBasicAuth(config.AuthUsername, config.AuthPassword)
	} else if config.AuthStrategy == "token" && config.AuthToken != "" {
		req.Header.Add("Authorization", "Bearer "+config.AuthToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func generateSurrogateKey(records []interface{}, config util.Config) {
	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			data := config.UniqueKey + r[config.PrimaryBookmark].(string)
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

func GenerateRecords(config util.Config) []interface{} {
	var responseMap map[string]interface{}

	apiResponse, err := callAPI(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error calling API: %v\n", err)
		os.Exit(1)
	}

	output := string(apiResponse)
	responseMapRecordsPath := config.ResponseRecordsPath
	if len(config.ResponseRecordsPath) == 0 && output[0:1] == "{" {
		output = "{\"results\":[" + output + "]}"
		responseMapRecordsPath = []string{"results"}
	} else if len(config.ResponseRecordsPath) == 0 && output[0:1] == "[" {
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
	if config.Pagination && config.PaginationStrategy == "next" {
		nextURL := getValueAtPath(config.PaginationNextPath, responseMap)
		if nextURL == nil {
			generateSurrogateKey(records, config)
			return records
		}

		nextConfig := config
		nextConfig.URL = nextURL.(string)
		records = append(records, GenerateRecords(nextConfig)...)
	}

	generateSurrogateKey(records, config)

	return records

}

func GenerateRecordMessages(records []interface{}, config util.Config) {
	var bookmark string
	if config.Bookmark && config.PrimaryBookmark != "" {
		bookmark = readBookmark(config)
	} else {
		bookmark = ""
	}

	for _, record := range records {
		r, _ := record.(map[string]interface{})

		if r[config.PrimaryBookmark].(string) > bookmark {
			message := util.Message{
				Type:          "RECORD",
				Data:          r,
				Stream:        config.URL + "__" + strings.Join(config.ResponseRecordsPath, "__"),
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
