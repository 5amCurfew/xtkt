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

func callAPI(config util.Config) ([]byte, error) {
	req, _ := http.NewRequest("GET", config.URL, nil)

	if config.AuthStrategy == "basic" && config.AuthUsername != "" && config.AuthPassword != "" {
		req.SetBasicAuth(config.AuthUsername, config.AuthPassword)
	} else if config.AuthStrategy == "token" && config.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+config.AuthToken)
	}
	req.Header.Set("Accept", "application/json")

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
			h := sha256.New()
			h.Write([]byte(util.ToString(util.GetValueAtPath(config.UniqueKeyPath, r)) + util.ToString(util.GetValueAtPath(config.PrimaryBookmarkPath, r))))

			hashBytes := h.Sum(nil)

			r["surrogate_key"] = hex.EncodeToString(hashBytes)
			r["time_extracted"] = time.Now()
		}
	}
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

	if len(responseMapRecordsPath) == 0 && output[0:1] == "{" {
		output = "{\"results\":[" + output + "]}"
		responseMapRecordsPath = []string{"results"}
	} else if len(responseMapRecordsPath) == 0 && output[0:1] == "[" {
		output = "{\"results\":" + output + "}"
		responseMapRecordsPath = []string{"results"}
	}

	json.Unmarshal([]byte(output), &responseMap)

	records, ok := util.GetValueAtPath(responseMapRecordsPath, responseMap).([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	// PAGINATED, "next"
	if config.Pagination && config.PaginationStrategy == "next" {
		nextURL := util.GetValueAtPath(config.PaginationNextPath, responseMap)
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
	if config.Bookmark && len(config.PrimaryBookmarkPath) > 0 {
		bookmark = readBookmark(config)
	} else {
		bookmark = ""
	}

	for _, record := range records {
		r, _ := record.(map[string]interface{})

		if util.ToString(util.GetValueAtPath(config.PrimaryBookmarkPath, r)) > bookmark {
			message := util.Message{
				Type:          "RECORD",
				Data:          r,
				Stream:        util.GenerateStreamName(config),
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
