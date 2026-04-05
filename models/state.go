package models

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

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
	PreviousBookmark        Bookmark `json:"-"`
}

var State StreamState
var bookmarkUpdates chan BookmarkUpdate
var bookmarkUpdaterWG sync.WaitGroup

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
		UpdatedAt: util.NowTimestamp(),
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

	if s.Bookmark.Latest == nil {
		s.Bookmark.Latest = map[string]BookmarkEntry{}
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
	s.LastExtractionStartedAt = util.NowTimestamp()
	s.PreviousBookmark = s.Bookmark.Clone()
}

// StartBookmarkUpdates starts the single-writer goroutine that owns bookmark mutations.
func (s *StreamState) StartBookmarkUpdates() {
	if bookmarkUpdates != nil {
		return
	}

	if s.Bookmark.Latest == nil {
		s.Bookmark.Latest = map[string]BookmarkEntry{}
	}

	bookmarkUpdates = make(chan BookmarkUpdate, 1024)
	bookmarkUpdaterWG.Add(1)

	go func() {
		defer bookmarkUpdaterWG.Done()
		for update := range bookmarkUpdates {
			s.applyBookmarkUpdate(update)
		}
	}()
}

// StopBookmarkUpdates drains outstanding bookmark updates before final state persistence.
func (s *StreamState) StopBookmarkUpdates() {
	if bookmarkUpdates == nil {
		return
	}

	close(bookmarkUpdates)
	bookmarkUpdaterWG.Wait()
	bookmarkUpdates = nil
}

// QueueBookmarkUpdate enqueues a bookmark mutation for the current extraction run.
func (s *StreamState) QueueBookmarkUpdate(record map[string]interface{}, emitted bool) {
	update := BookmarkUpdate{
		NaturalKey:   record["_sdc_natural_key"],
		SurrogateKey: record["_sdc_surrogate_key"].(string),
		Timestamp:    util.NowTimestamp(),
		Emitted:      emitted,
	}

	if bookmarkUpdates == nil {
		s.applyBookmarkUpdate(update)
		return
	}

	bookmarkUpdates <- update
}

func (s *StreamState) applyBookmarkUpdate(update BookmarkUpdate) {
	if s.Bookmark.Latest == nil {
		s.Bookmark.Latest = map[string]BookmarkEntry{}
	}

	// Convert natural key to string for bookmark storage (avoiding scientific notation)
	key := util.ToKeyString(update.NaturalKey)
	entry := s.Bookmark.Latest[key]
	if update.Emitted {
		entry.LastEmitted = update.Timestamp
	}

	entry.SurrogateKey = update.SurrogateKey
	entry.LastSeen = update.Timestamp
	s.Bookmark.Latest[key] = entry
	s.Bookmark.UpdatedAt = update.Timestamp
}

// BookmarkEntry tracks the surrogate key and last seen timestamp for a record
type BookmarkEntry struct {
	SurrogateKey string `json:"surrogate_key"`
	LastSeen     string `json:"last_seen"`
	LastEmitted  string `json:"last_emitted,omitempty"`
}

type BookmarkUpdate struct {
	NaturalKey   interface{}
	SurrogateKey string
	Timestamp    string
	Emitted      bool
}

type Bookmark struct {
	UpdatedAt string                   `json:"updated_at"`
	Latest    map[string]BookmarkEntry `json:"latest"`
}

func (b Bookmark) Clone() Bookmark {
	latest := make(map[string]BookmarkEntry, len(b.Latest))
	for key, entry := range b.Latest {
		latest[key] = entry
	}

	return Bookmark{
		UpdatedAt: b.UpdatedAt,
		Latest:    latest,
	}
}
