package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func GenerateSchemaMessage(schema map[string]interface{}, config util.Config) {
	message := util.Message{
		Type:          "SCHEMA",
		Stream:        *config.StreamName,
		TimeExtracted: time.Now().Format(time.RFC3339),
		Schema:        schema,
		KeyProperties: []string{"surrogate_key"},
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating SCHEMA message: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(messageJson))
}

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

func GenerateStateMessage() {
	stateFile, _ := os.ReadFile("state.json")
	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	message := util.Message{
		Type:          "STATE",
		Value:         state["value"],
		TimeExtracted: time.Now().Format(time.RFC3339),
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating STATE message: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(messageJson))
}
