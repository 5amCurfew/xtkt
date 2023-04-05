package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"

	util "github.com/5amCurfew/xtkt/util"
)

func generateSurrogateKey(records []interface{}, config util.Config) {
	if len(records) > 0 {
		for _, record := range records {
			h := sha256.New()
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

	emptyRecords := len(records) == 0

	if *config.Response.Pagination {
		switch *config.Response.PaginationStrategy {
		// PAGINATED, "next"
		case "next":
			nextURL := util.GetValueAtPath(*config.Response.PaginationNextPath, responseMap)
			if nextURL == nil || nextURL == "" {
				generateSurrogateKey(records, config)
				return records
			} else {
				*config.URL = nextURL.(string)
				records = append(records, GenerateRecords(config)...)
			}
		// PAGINATED, "query"
		case "query":
			if emptyRecords {
				generateSurrogateKey(records, config)
				return records
			} else {
				parsedURL, _ := url.Parse(*config.URL)
				query := parsedURL.Query()
				query.Set("page", strconv.Itoa(*config.Response.PaginationQuery.QueryValue))
				parsedURL.RawQuery = query.Encode()

				*config.URL = parsedURL.String()
				*config.Response.PaginationQuery.QueryValue = *config.Response.PaginationQuery.QueryValue + *config.Response.PaginationQuery.QueryIncrement
				records = append(records, GenerateRecords(config)...)
			}
		}
	}

	generateSurrogateKey(records, config)
	return records
}
