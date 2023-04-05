package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func BookmarkSet(config util.Config) bool {
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

// ///////////////////////////////////////////////////////////
// GENERATE/UPDATE/READ STATE
// ///////////////////////////////////////////////////////////
func CreateBookmark(config util.Config) {
	stream := make(map[string]interface{})
	data := make(map[string]interface{})

	data["bookmark_updated_at"] = ""
	data["detection_bookmark"] = []string{}
	data["primary_bookmark"] = ""

	stream[util.GenerateStreamName(URLsParsed[0], config)] = data

	values := make(map[string]interface{})
	values["bookmarks"] = stream

	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": values,
	})

	os.WriteFile("state.json", result, 0644)
}

func readBookmarkValue(config util.Config) interface{} {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	if reflect.DeepEqual(*config.Records.PrimaryBookmarkPath, []string{"*"}) {
		return state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[util.GenerateStreamName(URLsParsed[0], config)].(map[string]interface{})["detection_bookmark"]
	} else {
		return state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[util.GenerateStreamName(URLsParsed[0], config)].(map[string]interface{})["primary_bookmark"]
	}
}

func UpdateBookmark(records []interface{}, config util.Config) {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	// CURRENT
	latestBookmark := readBookmarkValue(config)

	// FIND LATEST
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		if util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r)) >= latestBookmark.(string) {
			latestBookmark = util.ToString(util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r))
		}
	}

	// UPDATE
	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[util.GenerateStreamName(URLsParsed[0], config)].(map[string]interface{})["primary_bookmark"] = latestBookmark

	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[util.GenerateStreamName(URLsParsed[0], config)].(map[string]interface{})["bookmark_updated_at"] = time.Now()

	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": state["value"],
	})

	os.WriteFile("state.json", result, 0644)
}

func UpdateDetectionBookmark(records []interface{}, config util.Config) {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	// CURRENT SET
	latestBookmark := readBookmarkValue(config).([]interface{})

	// FIND LATEST
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		if !detectionSetContains(latestBookmark, r["surrogate_key"]) {
			latestBookmark = append(latestBookmark, r["surrogate_key"])
		}
	}

	// UPDATE
	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[util.GenerateStreamName(URLsParsed[0], config)].(map[string]interface{})["detection_bookmark"] = latestBookmark

	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[util.GenerateStreamName(URLsParsed[0], config)].(map[string]interface{})["bookmark_updated_at"] = time.Now()

	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": state["value"],
	})

	os.WriteFile("state.json", result, 0644)
}

func GenerateStateMessage() {
	stateFile, _ := os.ReadFile("state.json")
	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	message := util.Message{
		Type:          "STATE",
		Value:         state["value"],
		TimeExtracted: time.Now().Format(time.RFC3339),
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating STATE message: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(messageJson))
}
