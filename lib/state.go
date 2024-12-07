package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

var stateMutex sync.Mutex
var ParsedState *State

// TODO MOVE BOOKMARKS TO MAP OF SEEN KEYS
type State struct {
	Type  string `json:"type"`
	Value struct {
		Bookmarks map[string]Bookmark `json:"bookmarks"`
	} `json:"value"`
}

type Bookmark struct {
	BookmarkUpdatedAt string              `json:"bookmark_updated_at"`
	Bookmark          map[string]struct{} `json:"bookmark"`
}

// CreateStateJSON creates a state JSON file for the stream
func CreateStateJSON() {
	// Ensure ParsedConfig is initialized and stream name is not nil
	if ParsedConfig.StreamName == nil {
		fmt.Println("Error: ParsedConfig.StreamName is nil")
		return
	}

	streamName := *ParsedConfig.StreamName

	// Initialize the state object
	state := State{
		Type: "STATE",
		Value: struct {
			Bookmarks map[string]Bookmark `json:"bookmarks"`
		}{
			Bookmarks: map[string]Bookmark{
				streamName: {
					BookmarkUpdatedAt: time.Now().UTC().Format(time.RFC3339),
					Bookmark:          map[string]struct{}{},
				},
			},
		},
	}

	// Write the state to a JSON file
	fileName := fmt.Sprintf("%s_state.json", streamName)
	err := util.WriteJSON(fileName, state)
	if err != nil {
		fmt.Printf("Error writing JSON: %v\n", err)
	}
}

// Parse <STREAM>_state.json
func ParseStateJSON() (*State, error) {
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
func UpdateStateBookmark(record interface{}) {
	stateMutex.Lock() // Prevent concurrent read/writes to state
	defer stateMutex.Unlock()

	// Access and modify the map
	bookmarks := ParsedState.Value.Bookmarks[*ParsedConfig.StreamName]
	r := record.(map[string]interface{})
	key, _ := r["_sdc_surrogate_key"].(string)

	// Modify the state
	bookmarks.Bookmark[key] = struct{}{}
	bookmarks.BookmarkUpdatedAt = time.Now().UTC().Format(time.RFC3339)

	// Update the map
	ParsedState.Value.Bookmarks[*ParsedConfig.StreamName] = bookmarks
}
