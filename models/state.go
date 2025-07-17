package models

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

var StateMutex sync.RWMutex
var State StreamState

type StreamState struct {
	Stream   string   `json:"stream"`
	Bookmark Bookmark `json:"bookmark"`
}

type Bookmark struct {
	UpdatedAt string            `json:"updated_at"`
	Latest    map[string]string `json:"latest"`
}

// Create creates a state JSON file for the stream
func (s *StreamState) Create() error {
	s.Stream = STREAM_NAME
	s.Bookmark = Bookmark{
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Latest:    map[string]string{},
	}

	if s.Stream == "" {
		return fmt.Errorf("state json stream name is nil")
	}

	fileName := fmt.Sprintf("%s_state.json", s.Stream)
	err := util.WriteJSON(fileName, s)
	if err != nil {
		return fmt.Errorf("state json writing to json file error: %v", err)
	}

	return nil
}

// Reads State JSON file
func (s *StreamState) Read() error {
	stateFile, err := os.ReadFile(fmt.Sprintf("%s_state.json", STREAM_NAME))
	if err != nil {
		return fmt.Errorf("error reading state file: %w", err)
	}

	if err := json.Unmarshal(stateFile, s); err != nil {
		return fmt.Errorf("error unmarshaling state json: %w", err)
	}

	return nil
}

// Updates the State JSON file
func (s *StreamState) Update(record map[string]interface{}) {
	StateMutex.Lock()
	defer StateMutex.Unlock()

	key := fmt.Sprintf("%v", record["_sdc_natural_key"])
	s.Bookmark.Latest[key] = record["_sdc_surrogate_key"].(string)
	s.Bookmark.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
}

// Message generates a message with the current state
func (s *StreamState) Message() error {
	message := Message{
		Type:   "STATE",
		Stream: s.Stream,
		Value:  s.Bookmark,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error creating state message: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))

	return nil
}
