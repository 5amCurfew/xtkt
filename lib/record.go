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

var URLsParsed []string

// ///////////////////////////////////////////////////////////
// PARSE RECORDS
// ///////////////////////////////////////////////////////////
func callAPI(config util.Config) ([]byte, error) {
	req, _ := http.NewRequest("GET", *config.URL, nil)

	if *config.Auth.Required {
		if *config.Auth.Strategy == "basic" && config.Auth.Username != nil && config.Auth.Password != nil {
			req.SetBasicAuth(*config.Auth.Username, *config.Auth.Username)
		} else if *config.Auth.Strategy == "token" && config.Auth.Token != nil {
			req.Header.Set("Authorization", "Bearer "+*config.Auth.Token)
		}
	}

	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	URLsParsed = append(URLsParsed, *config.URL)

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func generateSurrogateKey(records []interface{}, config util.Config) {
	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			keyComponent := ""

			h := sha256.New()
			if config.PrimaryBookmarkPath != nil {
				keyComponent = util.ToString(util.GetValueAtPath(*config.PrimaryBookmarkPath, r))
			}

			h.Write([]byte(util.ToString(util.GetValueAtPath(*config.UniqueKeyPath, r)) + keyComponent))

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

	var responseMapRecordsPath []string
	if config.Response.RecordsPath == nil {
		responseMapRecordsPath = []string{"results"}
		if output[0:1] == "{" {
			output = "{\"results\":[" + output + "]}"
		} else {
			output = "{\"results\":" + output + "}"
		}
	} else {
		responseMapRecordsPath = *config.Response.RecordsPath
	}

	json.Unmarshal([]byte(output), &responseMap)

	records, ok := util.GetValueAtPath(responseMapRecordsPath, responseMap).([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	// PAGINATED, "next"
	if *config.Response.Pagination && *config.Response.PaginationStrategy == "next" {
		nextURL := util.GetValueAtPath(*config.Response.PaginationPath, responseMap)
		if nextURL == nil {
			generateSurrogateKey(records, config)
			return records
		}

		nextConfig := config
		*nextConfig.URL = nextURL.(string)
		records = append(records, GenerateRecords(nextConfig)...)
	}

	generateSurrogateKey(records, config)

	return records

}

func GenerateRecordMessages(records []interface{}, config util.Config) {
	if *config.Bookmark && config.PrimaryBookmarkPath != nil {
		bookmark := readBookmark(config)
		for _, record := range records {
			r, _ := record.(map[string]interface{})

			if util.ToString(util.GetValueAtPath(*config.PrimaryBookmarkPath, r)) > bookmark {
				message := util.Message{
					Type:          "RECORD",
					Data:          r,
					Stream:        util.GenerateStreamName(URLsParsed[0], config),
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
	} else {
		for _, record := range records {
			r, _ := record.(map[string]interface{})

			message := util.Message{
				Type:          "RECORD",
				Data:          r,
				Stream:        util.GenerateStreamName(URLsParsed[0], config),
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
