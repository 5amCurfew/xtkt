package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

// ///////////////////////////////////////////////////////////
// GENERATE/UPDATE/READ STATE
// ///////////////////////////////////////////////////////////
func CreateBookmark(c util.Config) {
	stream := make(map[string]interface{})
	data := make(map[string]interface{})

	data["primary_bookmark"] = ""
	stream[c.URL+"__"+c.ResponseRecordsPath] = data

	values := make(map[string]interface{})
	values["bookmarks"] = stream

	result, _ := json.Marshal(map[string]interface{}{
		"type":  "STATE",
		"value": values,
	})

	os.WriteFile("state.json", result, 0644)
}

func readBookmark(c util.Config) string {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	return state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[c.URL+"__"+c.ResponseRecordsPath].(map[string]interface{})["primary_bookmark"].(string)
}

func UpdateBookmark(records []interface{}, c util.Config) {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	// CURRENT
	latestBookmark := readBookmark(c)

	// FIND LATEST
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		if r[c.PrimaryBookmark].(string) >= latestBookmark {
			latestBookmark = r[c.PrimaryBookmark].(string)
		}
	}

	// UPDATE
	state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[c.URL+"__"+c.ResponseRecordsPath].(map[string]interface{})["primary_bookmark"] = latestBookmark

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
		TimeExtracted: time.Now(),
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating STATE message: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(messageJson))
}
