package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

type State struct {
	Type  string `json:"Type"`
	Value struct {
		Bookmarks map[string]struct {
			BookmarkUpdatedAt string   `json:"bookmark_updated_at"`
			DetectionBookmark []string `json:"detection_bookmark"`
			PrimaryBookmark   string   `json:"primary_bookmark"`
		} `json:"bookmarks"`
	} `json:"Value"`
}

func UsingBookmark(config Config) bool {
	return *config.Records.Bookmark && config.Records.PrimaryBookmarkPath != nil
}

func detectionSetContains(s []string, str string) bool {
	// Sort the slice of strings
	sort.Strings(s)

	// Perform a binary search on the sorted slice
	index := sort.SearchStrings(s, str)
	if index < len(s) && s[index] == str {
		return true
	}

	return false
}

func WriteStateJSON(state *State) {
	result, _ := json.Marshal(state)
	os.WriteFile("state.json", result, 0644)
}

// ///////////////////////////////////////////////////////////
// CREATE
// ///////////////////////////////////////////////////////////
func CreateStateJSON(config Config) error {
	state := State{
		Type: "STATE",
		Value: struct {
			Bookmarks map[string]struct {
				BookmarkUpdatedAt string   `json:"bookmark_updated_at"`
				DetectionBookmark []string `json:"detection_bookmark"`
				PrimaryBookmark   string   `json:"primary_bookmark"`
			} `json:"bookmarks"`
		}{
			Bookmarks: map[string]struct {
				BookmarkUpdatedAt string   `json:"bookmark_updated_at"`
				DetectionBookmark []string `json:"detection_bookmark"`
				PrimaryBookmark   string   `json:"primary_bookmark"`
			}{
				*config.StreamName: {
					BookmarkUpdatedAt: time.Now().Format(time.RFC3339),
					DetectionBookmark: []string{},
					PrimaryBookmark:   "",
				},
			},
		},
	}

	result, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("error MARSHALLING STATE into JSON: %w", err)
	}

	if err := os.WriteFile("state.json", result, 0644); err != nil {
		return fmt.Errorf("error WRITING STATE.JSON: %w", err)
	}

	return nil
}

// ///////////////////////////////////////////////////////////
// PARSE STATE.JSON
// ///////////////////////////////////////////////////////////
func ParseStateJSON(config Config) (*State, error) {
	stateFile, err := os.ReadFile("state.json")
	if err != nil {
		return nil, fmt.Errorf("error reading state file: %w", err)
	}

	var state State
	if err := json.Unmarshal(stateFile, &state); err != nil {
		return nil, fmt.Errorf("error unmarshaling state JSON: %w", err)
	}

	if _, ok := state.Value.Bookmarks[*config.StreamName]; !ok {
		return nil, fmt.Errorf("stream %s does not exist in this state", *config.StreamName)
	}

	return &state, nil
}

// ///////////////////////////////////////////////////////////
// UPDATE
// ///////////////////////////////////////////////////////////
func UpdateStateUpdatedAt(state *State, config Config) error {
	bookmarks := state.Value.Bookmarks[*config.StreamName]
	bookmarks.BookmarkUpdatedAt = time.Now().Format(time.RFC3339)
	state.Value.Bookmarks[*config.StreamName] = bookmarks

	return nil
}

func UpdateBookmarkPrimary(records []interface{}, state *State, config Config) error {
	// CURRENT
	latestBookmark := state.Value.Bookmarks[*config.StreamName].PrimaryBookmark

	// FIND LATEST
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		if getValueAtPath(*config.Records.PrimaryBookmarkPath, r) == nil {
			continue
		} else if toString(getValueAtPath(*config.Records.PrimaryBookmarkPath, r)) >= latestBookmark {
			latestBookmark = toString(getValueAtPath(*config.Records.PrimaryBookmarkPath, r))
		}
	}

	// UPDATE PRIMARY BOOKMARK
	bookmarks := state.Value.Bookmarks[*config.StreamName]
	bookmarks.PrimaryBookmark = latestBookmark
	state.Value.Bookmarks[*config.StreamName] = bookmarks

	return nil
}

func UpdateBookmarkDetection(records []interface{}, state *State, config Config) error {
	// CURRENT
	latestDetectionSet := state.Value.Bookmarks[*config.StreamName].DetectionBookmark

	// UPDATE DETECTION SET
	for _, record := range records {
		r, ok := record.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error parsing record to detection set")
		}
		if !detectionSetContains(latestDetectionSet, r["_sdc_surrogate_key"].(string)) {
			latestDetectionSet = append(latestDetectionSet, r["_sdc_surrogate_key"].(string))
		}
	}

	// UPDATE
	bookmarks := state.Value.Bookmarks[*config.StreamName]
	bookmarks.DetectionBookmark = latestDetectionSet
	state.Value.Bookmarks[*config.StreamName] = bookmarks

	return nil
}
