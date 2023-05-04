package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func UsingBookmark(config Config) bool {
	return *config.Records.Bookmark && config.Records.PrimaryBookmarkPath != nil
}

func detectionSetContains(s []interface{}, str interface{}) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func writeStateJSON(state map[string]interface{}) {
	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": state["value"],
	})
	os.WriteFile("state.json", result, 0644)
}

// ///////////////////////////////////////////////////////////
// CREATE
// ///////////////////////////////////////////////////////////
func CreateStateJSON(config Config) error {
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

// ///////////////////////////////////////////////////////////
// PARSE STATE.JSON
// ///////////////////////////////////////////////////////////
func parseStateJSON(config Config) (map[string]interface{}, error) {
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

	return state, nil
}

// ///////////////////////////////////////////////////////////
// UPDATE
// ///////////////////////////////////////////////////////////
func UpdateBookmarkPrimary(records []interface{}, config Config) error {
	state, err := parseStateJSON(config)
	if err != nil {
		return fmt.Errorf("error parsing state parseStateJSON() %w", err)
	}

	// CURRENT
	latestBookmark := state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["primary_bookmark"].(string)

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
	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["primary_bookmark"] = latestBookmark

	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["bookmark_updated_at"] = time.Now().Format(time.RFC3339)

	writeStateJSON(state)
	return nil
}

func UpdateBookmarkDetection(records []interface{}, config Config) error {
	state, err := parseStateJSON(config)
	if err != nil {
		return fmt.Errorf("error parsing state parseStateJSON() %w", err)
	}

	// CURRENT
	latestDetectionSet := state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["detection_bookmark"].([]interface{})

	// UPDATE DETECTION SET
	for _, record := range records {
		r, ok := record.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error parsing record to detection set")
		}
		if !detectionSetContains(latestDetectionSet, r["_sdc_surrogate_key"]) {
			latestDetectionSet = append(latestDetectionSet, r["_sdc_surrogate_key"])
		}
	}

	// UPDATE
	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["detection_bookmark"] = latestDetectionSet

	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[*config.StreamName].(map[string]interface{})["bookmark_updated_at"] = time.Now().Format(time.RFC3339)

	writeStateJSON(state)
	return nil
}
