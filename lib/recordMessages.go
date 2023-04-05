package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func addMetadata(record map[string]interface{}) {
	record["time_extracted"] = time.Now().Format(time.RFC3339)
}

func GenerateRecordMessages(records []interface{}, config util.Config) {

	bookmark := readBookmarkValue(config)
	bookmarkCondition := false

	for _, record := range records {

		r, _ := record.(map[string]interface{})

		addMetadata(r)

		if *config.Records.Bookmark && config.Records.PrimaryBookmarkPath != nil {
			func(r map[string]interface{}) {
				bookmarkCondition = util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r)) > bookmark.(string)
			}(r)
		} else if *config.Records.Bookmark && reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
			func(r map[string]interface{}) {
				bookmarkCondition = !detectionSetContains(bookmark.([]interface{}), r["surrogate_key"])
			}(r)
		} else {
			func(r map[string]interface{}) {
				bookmarkCondition = true
			}(r)
		}

		if bookmarkCondition {
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
