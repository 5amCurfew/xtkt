package models

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

// Compile-time verification that StreamState implements Model interface
var _ Model = (*StreamState)(nil)

// StreamState represents a stream's extraction state and implements the Model interface.
// It maintains bookmarks for incremental extraction, tracking the latest processed
// records to enable resumable and incremental data extraction.
type StreamState struct {
	Stream                  string   `json:"stream"`
	LastExtractionStartedAt string   `json:"last_extraction_started_at,omitempty"`
	Bookmark                Bookmark `json:"bookmark"`
}

var StateMutex sync.RWMutex
var State StreamState

// Create creates a state JSON file for the stream
func (s *StreamState) Create(source ...interface{}) error {
	// Check if file already exists
	if _, err := os.Stat(fmt.Sprintf("%s_state.json", STREAM_NAME)); err == nil {
		// File exists, read it instead of creating new
		return s.Read()
	}

	s.Stream = STREAM_NAME
	if s.Stream == "" {
		return fmt.Errorf("error creating state file: stream name is required")
	}
	s.Bookmark = Bookmark{
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Latest:    map[string]BookmarkEntry{},
	}

	fileName := fmt.Sprintf("%s_state.json", s.Stream)
	err := util.WriteJSON(fileName, s)
	if err != nil {
		return fmt.Errorf("error writing state.json: %v", err)
	}

	return nil
}

// Read reads the State JSON file
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

// Update writes the current state to the JSON file
func (s *StreamState) Update() error {
	fileName := fmt.Sprintf("%s_state.json", s.Stream)
	err := util.WriteJSON(fileName, s)
	if err != nil {
		return fmt.Errorf("error updating state.json: %v", err)
	}
	return nil
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

// StartExtraction sets the extraction start timestamp for this run
func (s *StreamState) StartExtraction() {
	StateMutex.Lock()
	defer StateMutex.Unlock()
	s.LastExtractionStartedAt = time.Now().UTC().Format(time.RFC3339)
}

// UpdateBookmark updates the bookmark with information from a record
func (s *StreamState) UpdateBookmark(record map[string]interface{}) {
	StateMutex.Lock()
	defer StateMutex.Unlock()

	// Convert natural key to string for bookmark storage (avoiding scientific notation)
	key := util.ToKeyString(record["_sdc_natural_key"])
	timestamp := time.Now().UTC().Format(time.RFC3339)
	
	s.Bookmark.Latest[key] = BookmarkEntry{
		SurrogateKey: record["_sdc_surrogate_key"].(string),
		LastSeen:     timestamp,
	}
	s.Bookmark.UpdatedAt = timestamp
}

// BookmarkEntry tracks the surrogate key and last seen timestamp for a record
type BookmarkEntry struct {
	SurrogateKey string `json:"surrogate_key"`
	LastSeen     string `json:"last_seen"`
}

type Bookmark struct {
	UpdatedAt string                   `json:"updated_at"`
	Latest    map[string]BookmarkEntry `json:"latest"`
}
