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
// CREATE
// ///////////////////////////////////////////////////////////
func CreateStateJSON(config Config) {
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
				*config.StreamName: {
					BookmarkUpdatedAt: time.Now().UTC().Format(time.RFC3339),
					DetectionBookmark: []string{},
					Bookmark:          "",
				},
			},
		},
	}
	util.WriteJSON(fmt.Sprintf("state_%s.json", *config.StreamName), state)
}

// ///////////////////////////////////////////////////////////
// PARSE state_<STREAM>.json
// ///////////////////////////////////////////////////////////
func ParseStateJSON(config Config) (*State, error) {
	stateFile, err := os.ReadFile(fmt.Sprintf("state_%s.json", *config.StreamName))
	if err != nil {
		return nil, fmt.Errorf("error reading state file: %w", err)
	}

	var state State
	if err := json.Unmarshal(stateFile, &state); err != nil {
		return nil, fmt.Errorf("error unmarshaling state json: %w", err)
	}

	if _, ok := state.Value.Bookmarks[*config.StreamName]; !ok {
		return nil, fmt.Errorf("stream %s does not exist in this state", *config.StreamName)
	}

	return &state, nil
}

// ///////////////////////////////////////////////////////////
// UPDATE
// ///////////////////////////////////////////////////////////
func UpdateState(records []interface{}, state *State, config Config) {
	// CURRENT
	bookmarks := state.Value.Bookmarks[*config.StreamName]

	if config.Records.BookmarkPath != nil {
		switch path := *config.Records.BookmarkPath; {
		case reflect.DeepEqual(path, []string{"*"}):
			latestDetectionSet := state.Value.Bookmarks[*config.StreamName].DetectionBookmark
			for _, record := range records {
				r, _ := record.(map[string]interface{})
				if !detectionSetContains(latestDetectionSet, r["_sdc_surrogate_key"].(string)) {
					latestDetectionSet = append(latestDetectionSet, r["_sdc_surrogate_key"].(string))
				}
			}
			bookmarks.DetectionBookmark = latestDetectionSet
		default:
			latestBookmark := state.Value.Bookmarks[*config.StreamName].Bookmark
			for _, record := range records {
				r, _ := record.(map[string]interface{})
				if util.GetValueAtPath(*config.Records.BookmarkPath, r) == nil {
					continue
				} else if toString(util.GetValueAtPath(*config.Records.BookmarkPath, r)) >= latestBookmark {
					latestBookmark = toString(util.GetValueAtPath(*config.Records.BookmarkPath, r))
				}
			}
			bookmarks.Bookmark = latestBookmark
		}
	}

	bookmarks.BookmarkUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	state.Value.Bookmarks[*config.StreamName] = bookmarks
	util.WriteJSON(fmt.Sprintf("state_%s.json", *config.StreamName), state)
}

func detectionSetContains(s []string, str string) bool {
	sort.Strings(s)

	index := sort.SearchStrings(s, str)
	if index < len(s) && s[index] == str {
		return true
	}

	return false
}
