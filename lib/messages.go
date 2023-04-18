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

func GenerateSchemaMessage(schema map[string]interface{}, config Config) error {
	message := Message{
		Type:          "SCHEMA",
		Stream:        *config.StreamName,
		Schema:        schema,
		KeyProperties: []string{"_sdc_surrogate_key"},
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error creating Schema message: %w", err)

	}

	os.Stdout.Write(messageJson)
	return nil
}

func GenerateRecordMessage(record map[string]interface{}, config Config) error {

	bookmarkCondition := false

	bookmark, err := readBookmarkValue(config)
	if err != nil {
		return fmt.Errorf("error PARSING STATE WHEN GENERATING RECORDS: %w", err)
	}

	if IsBookmarkProvided(config) {
		if IsRecordDetectionProvided(config) {
			bookmarkCondition = !detectionSetContains(bookmark.([]interface{}), record["_sdc_surrogate_key"])
		} else {
			primaryBookmarkValue := getValueAtPath(*config.Records.PrimaryBookmarkPath, record)
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
			return fmt.Errorf("error creating RECORD message: %w", err)
		}

		os.Stdout.Write(messageJson)
	}
	return nil
}

func GenerateStateMessage() error {
	stateFile, _ := os.ReadFile("state.json")
	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	message := Message{
		Type:  "STATE",
		Value: state["value"],
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error creating STATE message: %w", err)

	}

	os.Stdout.Write(messageJson)
	return nil
}
