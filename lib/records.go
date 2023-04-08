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
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func generateSurrogateKey(records []interface{}, config util.Config) {
	for _, record := range records {
		r, ok := record.(map[string]interface{})
		if !ok {
			continue
		}

		r["natural_key"] = util.GetValueAtPath(*config.Records.UniqueKeyPath, r)

		h := sha256.New()
		if keyPath := config.Records.UniqueKeyPath; keyPath != nil {
			keyValue := util.GetValueAtPath(*keyPath, r)
			h.Write([]byte(util.ToString(keyValue)))
		}
		if bookmarkPath := config.Records.PrimaryBookmarkPath; bookmarkPath != nil {
			if reflect.DeepEqual(*bookmarkPath, []string{"*"}) {
				h.Write([]byte(util.ToString(r)))
			} else {
				bookmarkValue := util.ToString(util.GetValueAtPath(*bookmarkPath, r))
				if keyPath := config.Records.UniqueKeyPath; keyPath != nil {
					keyValue := util.ToString(util.GetValueAtPath(*keyPath, r))
					h.Write([]byte(keyValue + bookmarkValue))
				} else {
					h.Write([]byte(bookmarkValue))
				}
			}
		}
		r["surrogate_key"] = hex.EncodeToString(h.Sum(nil))
	}
}

func AddMetadata(records []interface{}, config util.Config) {
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		r["time_extracted"] = time.Now().Format(time.RFC3339)
	}
}

func GenerateRestRecords(config util.Config) []interface{} {
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
				records = append(records, GenerateRestRecords(config)...)
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
				records = append(records, GenerateRestRecords(config)...)
			}
		}
	}

	generateSurrogateKey(records, config)
	return records
}
