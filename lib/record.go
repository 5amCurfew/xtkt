package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func generateSurrogateKey(records []interface{}, config util.Config) {
	if len(records) > 0 {
		if reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
			for _, record := range records {
				r, _ := record.(map[string]interface{})
				h := sha256.New()

				h.Write([]byte(util.ToString(r)))
				hashBytes := h.Sum(nil)

				r["surrogate_key"] = hex.EncodeToString(hashBytes)
				r["time_extracted"] = time.Now()
			}
		} else {
			for _, record := range records {
				r, _ := record.(map[string]interface{})
				keyComponent := ""

				h := sha256.New()
				if config.Records.PrimaryBookmarkPath != nil {
					keyComponent = util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r))
				}

				h.Write([]byte(util.ToString(util.GetValueAtPath(*config.Records.UniqueKeyPath, r)) + keyComponent))

				hashBytes := h.Sum(nil)

				r["surrogate_key"] = hex.EncodeToString(hashBytes)
				r["time_extracted"] = time.Now()
			}
		}
	}
}

func GenerateRecords(config util.Config) []interface{} {
	var responseMap map[string]interface{}

	apiResponse, err := CallAPI(config)

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
	if *config.Response.Pagination {
		switch *config.Response.PaginationStrategy {
		case "next":
			nextURL := util.GetValueAtPath(*config.Response.PaginationNextPath, responseMap)
			if nextURL == nil {
				generateSurrogateKey(records, config)
				return records
			}

			nextConfig := config
			*nextConfig.URL = nextURL.(string)
			records = append(records, GenerateRecords(nextConfig)...)
		}
	}

	generateSurrogateKey(records, config)

	return records

}

func GenerateRecordMessages(records []interface{}, config util.Config) {
	bookmark := readBookmarkValue(config)

	//////////////////////////////////////
	// RECORD DETECTION
	/////////////////////////////////////
	if *config.Records.Bookmark && reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
		for _, record := range records {
			r, _ := record.(map[string]interface{})

			if !detectionSetContains(bookmark.([]string), r["detection_key"].(string)) {
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
	} else if *config.Records.Bookmark && config.Records.PrimaryBookmarkPath != nil {
		//////////////////////////////////////
		// USE BOOKMARK
		/////////////////////////////////////
		for _, record := range records {
			r, _ := record.(map[string]interface{})

			if util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r)) > bookmark.(string) {
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
		//////////////////////////////////////
		// ALL
		/////////////////////////////////////
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
