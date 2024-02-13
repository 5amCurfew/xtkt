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

// /////////////////////////////////////////////////////////
// STATE_<STREAM>.JSON
// /////////////////////////////////////////////////////////
type State struct {
	Type  string `json:"type"`
	Value struct {
		Bookmarks map[string]struct {
			BookmarkUpdatedAt string   `json:"bookmark_updated_at"`
			Bookmark          []string `json:"bookmark"`
		} `json:"bookmarks"`
	} `json:"Value"`
}

// ///////////////////////////////////////////////////////////
// CREATE state_<STREAM>.json
// ///////////////////////////////////////////////////////////
func CreateStateJSON() {
	state := State{
		Type: "STATE",
		Value: struct {
			Bookmarks map[string]struct {
				BookmarkUpdatedAt string   `json:"bookmark_updated_at"`
				Bookmark          []string `json:"bookmark"`
			} `json:"bookmarks"`
		}{
			Bookmarks: map[string]struct {
				BookmarkUpdatedAt string   `json:"bookmark_updated_at"`
				Bookmark          []string `json:"bookmark"`
			}{
				*ParsedConfig.StreamName: {
					BookmarkUpdatedAt: time.Now().UTC().Format(time.RFC3339),
					Bookmark:          []string{},
				},
			},
		},
	}
	util.WriteJSON(fmt.Sprintf("state_%s.json", *ParsedConfig.StreamName), state)
}

// ///////////////////////////////////////////////////////////
// PARSE state_<STREAM>.json
// ///////////////////////////////////////////////////////////
func ParseStateJSON() (*State, error) {
	stateFile, err := os.ReadFile(fmt.Sprintf("state_%s.json", *ParsedConfig.StreamName))
	if err != nil {
		return nil, fmt.Errorf("error reading state file: %w", err)
	}

	var state State
	if err := json.Unmarshal(stateFile, &state); err != nil {
		return nil, fmt.Errorf("error unmarshaling state json: %w", err)
	}

	if _, ok := state.Value.Bookmarks[*ParsedConfig.StreamName]; !ok {
		return nil, fmt.Errorf("stream %s does not exist in this state", *ParsedConfig.StreamName)
	}

	return &state, nil
}

// ///////////////////////////////////////////////////////////
// UPDATE state_<STREAM>.json
// ///////////////////////////////////////////////////////////
func UpdateState(record interface{}) {
	bookmarks := ParsedState.Value.Bookmarks[*ParsedConfig.StreamName]
	r := record.(map[string]interface{})
	key, _ := r["_sdc_surrogate_key"].(string)

	if !detectionSetContains(bookmarks.Bookmark, key) {
		bookmarks.Bookmark = append(bookmarks.Bookmark, key)
	}

	bookmarks.BookmarkUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	ParsedState.Value.Bookmarks[*ParsedConfig.StreamName] = bookmarks
	util.WriteJSON(fmt.Sprintf("state_%s.json", *ParsedConfig.StreamName), ParsedState)
}

func detectionSetContains(s []string, str string) bool {
	sort.Strings(s)

	index := sort.SearchStrings(s, str)
	if index < len(s) && s[index] == str {
		return true
	}

	return false
}
