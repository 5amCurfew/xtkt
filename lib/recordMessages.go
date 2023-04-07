package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func GenerateRecordMessage(record map[string]interface{}, config util.Config) {

	bookmarkCondition := false

	bookmark, _ := readBookmarkValue(config)

	if IsBookmarkProvided(config) {
		if IsRecordDetectionProvided(config) {
			bookmarkCondition = !detectionSetContains(bookmark.([]interface{}), record["surrogate_key"])
		} else {
			primaryBookmarkValue := util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, record)
			bookmarkCondition = util.ToString(primaryBookmarkValue) > bookmark.(string)
		}
	} else {
		bookmarkCondition = true
	}

	if bookmarkCondition {
		message := util.Message{
			Type:          "RECORD",
			Data:          record,
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
