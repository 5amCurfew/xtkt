package xtkt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type Config struct {
	Url                   string `json:"url"`
	Response_records_path string `json:"response_records_path"`
	Unique_key            string `json:"unique_key"`
	Bookmark              bool   `json:"bookmark"`
	Primary_bookmark      string `json:"primary_bookmark"`
}

type Record map[string]interface{}

type Message struct {
	Type               string      `json:"type"`
	Data               Record      `json:"record,omitempty"`
	Stream             string      `json:"stream,omitempty"`
	TimeExtracted      time.Time   `json:"time_extracted,omitempty"`
	Schema             interface{} `json:"schema,omitempty"`
	Value              interface{} `json:"value,omitempty"`
	KeyProperties      []string    `json:"key_properties,omitempty"`
	BookmarkProperties []string    `json:"bookmark_properties,omitempty"`
}

// ///////////////////////////////////////////////////////////
// GENERATE JSON SCHEMA
// ///////////////////////////////////////////////////////////
func generateSchema(records []interface{}) map[string]interface{} {

	schema := make(map[string]interface{})
	properties := make(map[string]interface{})

	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			for key, value := range r {
				if _, exists := properties[key]; !exists {
					properties[key] = make(map[string]interface{})
					switch value.(type) {
					case bool:
						properties[key].(map[string]interface{})["type"] = "boolean"
					case int:
						properties[key].(map[string]interface{})["type"] = "integer"
					case float64:
						properties[key].(map[string]interface{})["type"] = "number"
					case map[string]interface{}:
						subProps := generateSchema([]interface{}{value})
						properties[key].(map[string]interface{})["type"] = "object"
						properties[key].(map[string]interface{})["properties"] = subProps["properties"]
					case []interface{}:
						properties[key].(map[string]interface{})["type"] = "array"
					case nil:
						properties[key].(map[string]interface{})["type"] = "null"
					case string:
						if _, err := time.Parse(time.RFC3339, value.(string)); err == nil {
							properties[key].(map[string]interface{})["type"] = "timestamp"
							break
						} else if _, err := time.Parse("2006-01-02", value.(string)); err == nil {
							properties[key].(map[string]interface{})["type"] = "date"
							break
						} else {
							properties[key].(map[string]interface{})["type"] = "string"
						}
					}
				}
			}
		}
	}

	schema["properties"] = properties
	schema["type"] = "object"
	return schema
}

// ///////////////////////////////////////////////////////////
// GENERATE/UPDATE/READ STATE
// ///////////////////////////////////////////////////////////
func createBookmark(c Config) {
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

func readBookmark(c Config) string {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	return state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[c.Url+"__"+c.Response_records_path].(map[string]interface{})["primary_bookmark"].(string)
}

func updateBookmark(c Config, records []interface{}) {
	stateFile, _ := os.ReadFile("state.json")

	state := make(map[string]interface{})
	_ = json.Unmarshal(stateFile, &state)

	// CURRENT
	latestBookmark := readBookmark(c)

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

func generateSurrogateKey(c Config, records []interface{}) {
	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			data := c.Unique_key + r[c.Primary_bookmark].(string)
			h := sha256.New()
			h.Write([]byte(data))

			hashBytes := h.Sum(nil)

			r["surrogate_key"] = hex.EncodeToString(hashBytes)
		}
	}
}

// ///////////////////////////////////////////////////////////
// PARSE RECORDS (parse response > generate SCHEMA msg > generate RECORD msg(s) > handle STATE updates)
// ///////////////////////////////////////////////////////////
func ParseResponse(c Config) {
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

	generateSurrogateKey(c, records)

	/////////////////////////////////////////////////////////////
	// GENERATE BOOKMARK
	/////////////////////////////////////////////////////////////
	if c.Bookmark && c.Primary_bookmark != "" {
		if _, err := os.Stat("state.json"); os.IsNotExist(err) {
			createBookmark(c)
		}
	}

	/////////////////////////////////////////////////////////////
	// GENERATE SCHEMA Message
	/////////////////////////////////////////////////////////////
	message := Message{
		Type:               "SCHEMA",
		Stream:             c.Url + "__" + c.Response_records_path,
		TimeExtracted:      time.Now(),
		Schema:             generateSchema(records),
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

		message := Message{
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
		updateBookmark(c, records)

		stateFile, _ := os.ReadFile("state.json")
		state := make(map[string]interface{})
		_ = json.Unmarshal(stateFile, &state)

		message := Message{
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
