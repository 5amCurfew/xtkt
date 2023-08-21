package lib

import (
	"encoding/json"
	"fmt"
	"os"
)

// /////////////////////////////////////////////////////////
// MESSAGES
// /////////////////////////////////////////////////////////
type Message struct {
	Type               string                 `json:"type"`
	Record             map[string]interface{} `json:"record,omitempty"`
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

	if *config.SourceType == "listen" {
		fmt.Println(string(messageJson))
	} else {
		os.Stdout.Write(messageJson)
		os.Stdout.Write([]byte("\n"))
	}

	return nil
}

func GenerateRecordMessage(record interface{}, state *State, config Config) error {
	r, parsed := record.(map[string]interface{})
	if !parsed {
		return fmt.Errorf("error PARSING RECORD IN GenerateRecordMessage: %v", r)
	}

	message := Message{
		Type:   "RECORD",
		Record: r,
		Stream: *config.StreamName,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING RECORD MESSAGE: %w", err)
	}

	if *config.SourceType == "listen" {
		fmt.Println(string(messageJson))
	} else {
		os.Stdout.Write(messageJson)
		os.Stdout.Write([]byte("\n"))
	}

	return nil
}

func GenerateStateMessage(state *State, config Config) error {
	message := Message{
		Type:   "STATE",
		Stream: *config.StreamName,
		Value:  state.Value,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING STATE MESSAGE: %w", err)
	}

	if *config.SourceType == "listen" {
		fmt.Println(string(messageJson))
	} else {
		os.Stdout.Write(messageJson)
		os.Stdout.Write([]byte("\n"))
	}

	return nil
}
