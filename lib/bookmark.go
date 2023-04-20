package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"
)

func IsBookmarkProvided(config Config) bool {
	return *config.Records.Bookmark && config.Records.PrimaryBookmarkPath != nil
}

func IsRecordDetectionProvided(config Config) bool {
	return *config.Records.Bookmark && reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"})
}

func detectionSetContains(s []interface{}, str interface{}) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func readState() map[string]interface{} {
	stateFile, _ := os.ReadFile("state.json")
	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)
	return state
}

func readDetectionBookmark(state map[string]interface{}, config Config) []interface{} {
	bookmarks := state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})
	if reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
		return bookmarks["detection_bookmark"].([]interface{})
	} else {
		return []interface{}{bookmarks["primary_bookmark"]}
	}
}

func writeState(state map[string]interface{}) {
	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": state["value"],
	})
	os.WriteFile("state.json", result, 0644)
}

// ///////////////////////////////////////////////////////////
// GENERATE/UPDATE/READ STATE
// ///////////////////////////////////////////////////////////
func CreateBookmark(config Config) error {
	stream := make(map[string]interface{})
	data := make(map[string]interface{})

	data["bookmark_updated_at"] = time.Now().Format(time.RFC3339)
	data["detection_bookmark"] = []string{}
	data["primary_bookmark"] = ""

	stream[*config.StreamName] = data

	values := make(map[string]interface{})
	values["bookmarks"] = stream

	result, err := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": values,
	})
	if err != nil {
		return fmt.Errorf("error MARSHALLING STATE into JSON: %w", err)
	}

	if err := os.WriteFile("state.json", result, 0644); err != nil {
		return fmt.Errorf("error WRITING STATE.JSON: %w", err)
	}

	return nil
}

func readBookmarkValue(config Config) (interface{}, error) {
	stateFile, err := os.ReadFile("state.json")
	if err != nil {
		return nil, fmt.Errorf("error reading state file: %w", err)
	}

	state := make(map[string]interface{})
	if err := json.Unmarshal(stateFile, &state); err != nil {
		return nil, fmt.Errorf("error unmarshaling state JSON: %w", err)
	}

	if state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName] == nil {
		return nil, fmt.Errorf("error stream %s DOES NOT EXIST in this STATE.JSON", *config.StreamName)
	}

	bookmarks := state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})
	if reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
		return bookmarks["detection_bookmark"], nil
	} else {
		return bookmarks["primary_bookmark"], nil
	}
}

func UpdateBookmark(records []interface{}, config Config) error {
	stateFile, err := os.ReadFile("state.json")
	if err != nil {
		return fmt.Errorf("error READING STATE.JSON for stream %s", *config.StreamName)
	}

	state := make(map[string]interface{})
	if err := json.Unmarshal(stateFile, &state); err != nil {
		return fmt.Errorf("error PARSING STATE.JSON for stream %s: %v", *config.StreamName, err)
	}

	if state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName] == nil {
		return fmt.Errorf("error stream %s DOES NOT EXIST in this STATE.JSON", *config.StreamName)
	}

	// CURRENT
	latestBookmark, _ := readBookmarkValue(config)

	// FIND LATEST
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		if getValueAtPath(*config.Records.PrimaryBookmarkPath, r) == nil {
			latestBookmark = ""
		} else if toString(getValueAtPath(*config.Records.PrimaryBookmarkPath, r)) >= latestBookmark.(string) {
			latestBookmark = toString(getValueAtPath(*config.Records.PrimaryBookmarkPath, r))
		}
	}

	// UPDATE
	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["primary_bookmark"] = latestBookmark

	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["bookmark_updated_at"] = time.Now().Format(time.RFC3339)

	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": state["value"],
	})

	os.WriteFile("state.json", result, 0644)
	return nil
}

func UpdateDetectionBookmark(records []interface{}, config Config) error {
	state := readState()

	if state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName] == nil {
		return fmt.Errorf("error stream %s DOES NOT EXIST in this STATE.JSON", *config.StreamName)
	}

	// Current set
	latestBookmark := readDetectionBookmark(state, config)

	// Find latest
	for _, record := range records {
		r, ok := record.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error parsing record to detection set")
		}
		if !detectionSetContains(latestBookmark, r["_sdc_surrogate_key"]) {
			latestBookmark = append(latestBookmark, r["_sdc_surrogate_key"])
		}
	}

	// Update
	bookmarks := state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})
	bookmarks["detection_bookmark"] = latestBookmark
	bookmarks["bookmark_updated_at"] = time.Now().Format(time.RFC3339)

	writeState(state)
	return nil
}
