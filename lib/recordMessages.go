package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func GenerateRecordMessages(records []interface{}, config util.Config) {

	bookmarkCondition := false

	for _, record := range records {

		r, _ := record.(map[string]interface{})

		if IsBookmarkProvided(config) {
			bookmark := readBookmarkValue(config)
			if IsRecordDetectionProvided(config) {
				func(r map[string]interface{}) {
					bookmarkCondition = !detectionSetContains(bookmark.([]interface{}), r["surrogate_key"])
				}(r)
			} else {
				func(r map[string]interface{}) {
					bookmarkCondition = util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r)) > bookmark.(string)
				}(r)
			}
		} else {
			func(r map[string]interface{}) {
				bookmarkCondition = true
			}(r)
		}

		if bookmarkCondition {
			message := util.Message{
				Type:          "RECORD",
				Data:          r,
				Stream:        *config.StreamName,
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
