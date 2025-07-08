package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

var stateMutex sync.RWMutex
var ParsedState *State

type State struct {
	Type     string   `json:"type"`
	Stream   string   `json:"stream"`
	Bookmark Bookmark `json:"bookmark"`
}

type Bookmark struct {
	UpdatedAt  string            `json:"updated_at"`
	Latest     map[string]string `json:"latest"`
	Quarantine map[string]string `json:"quarantine"`
}

// CreateStateJSON creates a state JSON file for the stream
func CreateStateJSON() error {
	// Ensure ParsedConfig is initialized and stream name is not nil
	if ParsedConfig.StreamName == nil {
		return fmt.Errorf("state json ParsedConfig.StreamName is nil")
	}

	streamName := *ParsedConfig.StreamName

	// Initialize the state object
	state := State{
		Type:   "STATE",
		Stream: *ParsedConfig.StreamName,
		Bookmark: Bookmark{
			UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
			Latest:     map[string]string{},
			Quarantine: map[string]string{},
		},
	}

	// Write the state to a JSON file
	fileName := fmt.Sprintf("%s_state.json", streamName)
	err := util.WriteJSON(fileName, state)
	if err != nil {
		return fmt.Errorf("state json writing to json file error: %v", err)
	}

	return nil
}

// Reads <STREAM_NAME>_state.json
func ReadStateJSON() (*State, error) {
	stateFile, err := os.ReadFile(fmt.Sprintf("%s_state.json", *ParsedConfig.StreamName))
	if err != nil {
		return nil, fmt.Errorf("error reading state file: %w", err)
	}

	var state State
	if err := json.Unmarshal(stateFile, &state); err != nil {
		return nil, fmt.Errorf("error unmarshaling state json: %w", err)
	}

	return &state, nil
}

// Update <STREAM>_state.json
func UpdateStateBookmark(record map[string]interface{}) {
	stateMutex.Lock() // Prevent concurrent read/writes to state
	defer stateMutex.Unlock()

	// Access and modify the map
	bookmark := ParsedState.Bookmark
	key := fmt.Sprintf("%v", record["_sdc_natural_key"])

	// Modify the state
	bookmark.Latest[key] = record["_sdc_surrogate_key"].(string)
	bookmark.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	// Update the map
	ParsedState.Bookmark = bookmark
}

// Update <STREAM>_state.json
func UpdateStateQuarantine(record map[string]interface{}) {
	stateMutex.Lock() // Prevent concurrent read/writes to state
	defer stateMutex.Unlock()

	// Access and modify the map
	bookmark := ParsedState.Bookmark
	key := fmt.Sprintf("%v", record["_sdc_natural_key"])

	// Modify the state
	bookmark.Quarantine[key] = time.Now().UTC().Format(time.RFC3339)
	bookmark.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	// Update the map
	ParsedState.Bookmark = bookmark
}

// ProduceStateMessage generates a message with the current state
func ProduceStateMessage(state *State) error {
	message := Message{
		Type:   "STATE",
		Stream: *ParsedConfig.StreamName,
		Value:  state.Bookmark,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error creating state message: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))

	return nil
}
