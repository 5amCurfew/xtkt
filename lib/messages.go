package lib

import (
	"encoding/json"
	"fmt"
	"os"
)

type Message struct {
	Type               string                 `json:"type"`
	Data               map[string]interface{} `json:"record,omitempty"`
	Stream             string                 `json:"stream,omitempty"`
	TimeExtracted      string                 `json:"time_extracted,omitempty"`
	Schema             interface{}            `json:"schema,omitempty"`
	Value              interface{}            `json:"value,omitempty"`
	KeyProperties      []string               `json:"key_properties,omitempty"`
	BookmarkProperties []string               `json:"bookmark_properties,omitempty"`
}

func GenerateSchemaMessage(schema map[string]interface{}, config Config) {
	message := Message{
		Type:          "SCHEMA",
		Stream:        *config.StreamName,
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

func GenerateRecordMessage(record map[string]interface{}, config Config) {

	bookmarkCondition := false

	bookmark, _ := readBookmarkValue(config)

	if IsBookmarkProvided(config) {
		if IsRecordDetectionProvided(config) {
			bookmarkCondition = !detectionSetContains(bookmark.([]interface{}), record["surrogate_key"])
		} else {
			primaryBookmarkValue := GetValueAtPath(*config.Records.PrimaryBookmarkPath, record)
			bookmarkCondition = toString(primaryBookmarkValue) > bookmark.(string)
		}
	} else {
		bookmarkCondition = true
	}

	if bookmarkCondition {
		message := Message{
			Type:   "RECORD",
			Data:   record,
			Stream: *config.StreamName,
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

	message := Message{
		Type:  "STATE",
		Value: state["value"],
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating STATE message: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(messageJson))
}
