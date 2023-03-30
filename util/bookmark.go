package util

import (
	"encoding/json"
	"os"
)

// ///////////////////////////////////////////////////////////
// GENERATE/UPDATE/READ STATE
// ///////////////////////////////////////////////////////////
func CreateBookmark(c Config) {
	stream := make(map[string]interface{})
	data := make(map[string]interface{})

	data["primary_bookmark"] = ""
	stream[c.Url+"__"+c.Response_records_path] = data

	values := make(map[string]interface{})
	values["bookmarks"] = stream

	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": values,
	})

	os.WriteFile("state.json", result, 0644)
}

func ReadBookmark(c Config) string {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	return state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[c.Url+"__"+c.Response_records_path].(map[string]interface{})["primary_bookmark"].(string)
}

func UpdateBookmark(c Config, records []interface{}) {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	// CURRENT
	latestBookmark := ReadBookmark(c)

	// FIND LATEST
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		if r[c.Primary_bookmark].(string) >= latestBookmark {
			latestBookmark = r[c.Primary_bookmark].(string)
		}
	}

	// UPDATE
	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[c.Url+"__"+c.Response_records_path].(map[string]interface{})["primary_bookmark"] = latestBookmark

	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": state["value"],
	})

	os.WriteFile("state.json", result, 0644)
}
