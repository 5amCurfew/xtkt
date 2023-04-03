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

func removeNullFields(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		if v == nil {
			delete(m, k)
		} else if nestedMap, ok := v.(map[string]interface{}); ok {
			removeNullFields(nestedMap)
		}
	}
	return m
}

func generateSurrogateKey(records []interface{}, config util.Config) {
	h := sha256.New()

	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			if config.Records.PrimaryBookmarkPath != nil {
				if reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
					h.Write([]byte(util.ToString(r)))
				} else {
					keyComponent := util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r))
					h.Write([]byte(util.ToString(util.GetValueAtPath(*config.Records.UniqueKeyPath, r)) + keyComponent))
				}
			} else {
				h.Write([]byte(util.ToString(util.GetValueAtPath(*config.Records.UniqueKeyPath, r))))
			}
			hashBytes := h.Sum(nil)
			r["surrogate_key"] = hex.EncodeToString(hashBytes)
		}
	}
}

func AddMetadata(records []interface{}, config util.Config) []interface{} {
	if len(records) > 0 {
		for _, record := range records {
			record.(map[string]interface{})["time_extracted"] = time.Now().Format(time.RFC3339)
		}
	}
	return records
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
			if nextURL == nil || nextURL == "" {
				generateSurrogateKey(records, config)
				return records
			} else {
				*config.URL = nextURL.(string)
				records = append(records, GenerateRecords(config)...)
			}
		}
	}

	generateSurrogateKey(records, config)
	return records

}

func GenerateRecordMessages(records []interface{}, config util.Config) {
	//////////////////////////////////////
	// RECORD DETECTION
	/////////////////////////////////////
	if *config.Records.Bookmark && reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
		bookmark := readBookmarkValue(config).([]interface{})
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			r = removeNullFields(r)

			if !detectionSetContains(bookmark, r["surrogate_key"]) {
				message := util.Message{
					Type:          "RECORD",
					Data:          r,
					Stream:        util.GenerateStreamName(URLsParsed[0], config),
					TimeExtracted: time.Now().Format(time.RFC3339),
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
		bookmark := readBookmarkValue(config).(string)
		//////////////////////////////////////
		// USE BOOKMARK
		/////////////////////////////////////
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			r = removeNullFields(r)

			if util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r)) > bookmark {
				message := util.Message{
					Type:          "RECORD",
					Data:          r,
					Stream:        util.GenerateStreamName(URLsParsed[0], config),
					TimeExtracted: time.Now().Format(time.RFC3339),
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
			r = removeNullFields(r)
			message := util.Message{
				Type:          "RECORD",
				Data:          r,
				Stream:        util.GenerateStreamName(URLsParsed[0], config),
				TimeExtracted: time.Now().Format(time.RFC3339),
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
