package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

var ParsedState *State

type State struct {
	Type  string `json:"type"`
	Value struct {
		Bookmarks map[string]Bookmark `json:"bookmarks"`
	} `json:"value"`
}

type Bookmark struct {
	BookmarkUpdatedAt string   `json:"bookmark_updated_at"`
	Bookmark          []string `json:"bookmark"`
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
					Bookmark:          []string{}, // Empty bookmark list
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
func UpdateState(record interface{}) {
	bookmarks := ParsedState.Value.Bookmarks[*ParsedConfig.StreamName]
	r := record.(map[string]interface{})
	key, _ := r["_sdc_surrogate_key"].(string)

	if !detectionSetContains(bookmarks.Bookmark, key) {
		bookmarks.Bookmark = append(bookmarks.Bookmark, key)
	}

	bookmarks.BookmarkUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	ParsedState.Value.Bookmarks[*ParsedConfig.StreamName] = bookmarks
	util.WriteJSON(fmt.Sprintf("%s_state.json", *ParsedConfig.StreamName), ParsedState)
}

func detectionSetContains(s []string, str string) bool {
	sort.Strings(s)

	index := sort.SearchStrings(s, str)
	if index < len(s) && s[index] == str {
		return true
	}

	return false
}
