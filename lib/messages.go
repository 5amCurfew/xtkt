package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	log "github.com/sirupsen/logrus"
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
	os.Stdout.Write([]byte("\n"))
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
			bookmarkCondition = !detectionSetContains(
				state.Value.Bookmarks[*config.StreamName]["detection_bookmark"].([]interface{}),
				record["_sdc_surrogate_key"],
			)
		default:
			primaryBookmarkValue := getValueAtPath(*config.Records.PrimaryBookmarkPath, record)
			bookmarkCondition = toString(primaryBookmarkValue) > state.Value.Bookmarks[*config.StreamName]["primary_bookmark"].(string)
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
		os.Stdout.Write([]byte("\n"))
	}
	return nil
}

func GenerateMetricInfoMessage(records []interface{}, elapsed time.Duration, config Config) error {

	n := 0

	if UsingBookmark(config) {
		state, err := parseStateJSON(config)
		if err != nil {
			return fmt.Errorf("error PARSING STATE WHEN GENERATING RECORD MESSAGES: %w", err)
		}

		for _, record := range records {
			r := record.(map[string]interface{})
			switch path := *config.Records.PrimaryBookmarkPath; {
			case reflect.DeepEqual(path, []string{"*"}):
				if !detectionSetContains(
					state.Value.Bookmarks[*config.StreamName]["detection_bookmark"].([]interface{}),
					r["_sdc_surrogate_key"],
				) {
					n++
				}
			default:
				primaryBookmarkValue := getValueAtPath(*config.Records.PrimaryBookmarkPath, r)
				if toString(primaryBookmarkValue) > state.Value.Bookmarks[*config.StreamName]["primary_bookmark"].(string) {
					n++
				}
			}
		}
	} else {
		n = len(records)
	}

	message := Message{
		Type:   "METRIC",
		Stream: *config.StreamName,
		Data: map[string]interface{}{
			"record_messages": n,
			"time_elapsed":    elapsed,
		},
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING METRIC MESSAGE: %w", err)

	}

	log.Info(fmt.Sprintf("INFO METRIC: %s", messageJson))
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
		Value: state["Value"],
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING STATE MESSAGE: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))
	return nil
}
