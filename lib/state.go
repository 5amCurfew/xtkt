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

var ParsedState *State

// /////////////////////////////////////////////////////////
// STATE_<STREAM>.JSON
// /////////////////////////////////////////////////////////
type State struct {
	Type  string `json:"Type"`
	Value struct {
		Bookmarks map[string]struct {
			BookmarkUpdatedAt string   `json:"last_extraction_at"`
			DetectionBookmark []string `json:"detection_bookmark"`
			Bookmark          string   `json:"bookmark"`
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
				BookmarkUpdatedAt string   `json:"last_extraction_at"`
				DetectionBookmark []string `json:"detection_bookmark"`
				Bookmark          string   `json:"bookmark"`
			} `json:"bookmarks"`
		}{
			Bookmarks: map[string]struct {
				BookmarkUpdatedAt string   `json:"last_extraction_at"`
				DetectionBookmark []string `json:"detection_bookmark"`
				Bookmark          string   `json:"bookmark"`
			}{
				*ParsedConfig.StreamName: {
					BookmarkUpdatedAt: time.Now().UTC().Format(time.RFC3339),
					DetectionBookmark: []string{},
					Bookmark:          "",
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
	// CURRENT
	bookmarks := ParsedState.Value.Bookmarks[*ParsedConfig.StreamName]

	if ParsedConfig.Records.BookmarkPath != nil {
		switch path := *ParsedConfig.Records.BookmarkPath; {
		case reflect.DeepEqual(path, []string{"*"}):
			latestDetectionSet := ParsedState.Value.Bookmarks[*ParsedConfig.StreamName].DetectionBookmark
			r, _ := record.(map[string]interface{})
			if !detectionSetContains(latestDetectionSet, r["_sdc_surrogate_key"].(string)) {
				latestDetectionSet = append(latestDetectionSet, r["_sdc_surrogate_key"].(string))
			}
			bookmarks.DetectionBookmark = latestDetectionSet
		default:
			latestBookmark := ParsedState.Value.Bookmarks[*ParsedConfig.StreamName].Bookmark
			r, _ := record.(map[string]interface{})
			if toString(util.GetValueAtPath(*ParsedConfig.Records.BookmarkPath, r)) >= latestBookmark {
				latestBookmark = toString(util.GetValueAtPath(*ParsedConfig.Records.BookmarkPath, r))
			}
			bookmarks.Bookmark = latestBookmark
		}
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
