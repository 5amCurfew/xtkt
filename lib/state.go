package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"time"

	util "github.com/5amCurfew/xtkt/util"
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

func writeStateJSON(state *State) error {
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
					BookmarkUpdatedAt: time.Now().UTC().Format(time.RFC3339),
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

	return writeStateJSON(&state)
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
func UpdateState(records []interface{}, state *State, config Config) error {
	// CURRENT
	bookmarks := state.Value.Bookmarks[*config.StreamName]

	if config.Records.PrimaryBookmarkPath != nil {
		switch path := *config.Records.PrimaryBookmarkPath; {
		case reflect.DeepEqual(path, []string{"*"}):
			latestDetectionSet := state.Value.Bookmarks[*config.StreamName].DetectionBookmark
			for _, record := range records {
				r, parsed := record.(map[string]interface{})
				if !parsed {
					return fmt.Errorf("error PARSING RECORD IN UpdateStateBookmark (new-record-detection)")
				}
				if !detectionSetContains(latestDetectionSet, r["_sdc_surrogate_key"].(string)) {
					latestDetectionSet = append(latestDetectionSet, r["_sdc_surrogate_key"].(string))
				}
			}
			bookmarks.DetectionBookmark = latestDetectionSet
		default:
			latestBookmark := state.Value.Bookmarks[*config.StreamName].PrimaryBookmark
			for _, record := range records {
				r, parsed := record.(map[string]interface{})
				if !parsed {
					return fmt.Errorf("error PARSING RECORD IN UpdateStateBookmark (primary-bookmark)")
				}
				if util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r) == nil {
					continue
				} else if toString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r)) >= latestBookmark {
					latestBookmark = toString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r))
				}
			}
			bookmarks.PrimaryBookmark = latestBookmark
		}
	}

	// UPDATE
	bookmarks.BookmarkUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	state.Value.Bookmarks[*config.StreamName] = bookmarks

	return writeStateJSON(state)
}
