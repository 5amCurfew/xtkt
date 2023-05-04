package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
)

type Message struct {
	Type               string                 `json:"type"`
	Data               map[string]interface{} `json:"record,omitempty"`
	Stream             string                 `json:"stream,omitempty"`
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
		return fmt.Errorf("error CREATING SCHEMA MESSAGE: %w", err)

	}

	os.Stdout.Write(messageJson)
	return nil
}

func GenerateRecordMessage(record map[string]interface{}, config Config) error {

	bookmarkCondition := false

	if UsingBookmark(config) {
		state, err := parseStateJSON(config)
		if err != nil {
			return fmt.Errorf("error PARSING STATE WHEN GENERATING RECORD MESSAGES: %w", err)
		}

		switch path := *config.Records.PrimaryBookmarkPath; {
		case reflect.DeepEqual(path, []string{"*"}):
			bookmarkCondition = !detectionSetContains(state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["detection_bookmark"].([]interface{}), record["_sdc_surrogate_key"])
		default:
			primaryBookmarkValue := getValueAtPath(*config.Records.PrimaryBookmarkPath, record)
			bookmarkCondition = toString(primaryBookmarkValue) > state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["primary_bookmark"].(string)
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
			return fmt.Errorf("error CREATING RECORD MESSAGE: %w", err)
		}

		os.Stdout.Write(messageJson)
	}
	return nil
}

func GenerateStateMessage() error {
	stateFile, _ := os.ReadFile("state.json")
	state := make(map[string]interface{})
	if err := json.Unmarshal(stateFile, &state); err != nil {
		return fmt.Errorf("error unmarshaling state JSON: %w", err)
	}

	message := Message{
		Type:  "STATE",
		Value: state["value"],
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING STATE MESSAGE: %w", err)
	}

	os.Stdout.Write(messageJson)
	return nil
}
