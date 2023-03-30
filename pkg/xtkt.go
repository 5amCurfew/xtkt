package xtkt

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

// ///////////////////////////////////////////////////////////
// PARSE RECORDS (parse response > generate SCHEMA msg > generate RECORD msg(s) > handle STATE updates)
// ///////////////////////////////////////////////////////////
func ParseResponse(c util.Config) {
	var responseMap map[string]interface{}

	apiResponse, err := http.Get(c.Url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error calling API: %v\n", err)
		os.Exit(1)
	}

	defer apiResponse.Body.Close()

	body, err := io.ReadAll(apiResponse.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
	}

	output := string(body)

	if c.Response_records_path == "" && output[0:1] == "{" {
		output = "{\"results\":[" + output + "]}"
	} else if c.Response_records_path == "" && output[0:1] == "[" {
		output = "{\"results\":" + output + "}"
	}

	json.Unmarshal([]byte(output), &responseMap)

	records, ok := responseMap["results"].([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	util.GenerateSurrogateKey(c, records)

	/////////////////////////////////////////////////////////////
	// GENERATE BOOKMARK
	/////////////////////////////////////////////////////////////
	if c.Bookmark && c.Primary_bookmark != "" {
		if _, err := os.Stat("state.json"); os.IsNotExist(err) {
			util.CreateBookmark(c)
		}
	}

	/////////////////////////////////////////////////////////////
	// GENERATE SCHEMA Message
	/////////////////////////////////////////////////////////////
	message := util.Message{
		Type:               "SCHEMA",
		Stream:             c.Url + "__" + c.Response_records_path,
		TimeExtracted:      time.Now(),
		Schema:             util.GenerateSchema(records),
		KeyProperties:      []string{"surrogate_key"},
		BookmarkProperties: []string{c.Primary_bookmark},
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating SCHEMA message: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(messageJson))

	/////////////////////////////////////////////////////////////
	// GENERATE RECORD Message(s)
	/////////////////////////////////////////////////////////////
	for _, record := range records {
		r, _ := record.(map[string]interface{})

		message := util.Message{
			Type:          "RECORD",
			Data:          r,
			Stream:        c.Url + "__" + c.Response_records_path,
			TimeExtracted: time.Now(),
		}

		messageJson, err := json.Marshal(message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating RECORD message: %v\n", err)
			os.Exit(1)
		}

		fmt.Println(string(messageJson))
	}

	/////////////////////////////////////////////////////////////
	// GENERATE STATE Message (if required)
	/////////////////////////////////////////////////////////////
	if c.Bookmark && c.Primary_bookmark != "" {
		util.UpdateBookmark(c, records)

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

}
