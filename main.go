package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

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
// Create JSON Schema
// ///////////////////////////////////////////////////////////
func generateSchema(records []interface{}) map[string]interface{} {

	schema := make(map[string]interface{})
	properties := make(map[string]interface{})

	if len(records) > 0 {
		for _, record := range records {
			record, ok := record.(map[string]interface{})
			if !ok {
				fmt.Fprint(os.Stderr, "Error: record is not a map\n")
				os.Exit(1)
			}
			for key, value := range record {
				if _, ok := properties[key]; !ok {
					properties[key] = make(map[string]interface{})
					switch v := value.(type) {
					case bool:
						properties[key].(map[string]interface{})["type"] = "boolean"
					case int:
						properties[key].(map[string]interface{})["type"] = "integer"
					case float64:
						properties[key].(map[string]interface{})["type"] = "number"
					case map[string]interface{}:
						subProps := generateSchema([]interface{}{v})
						properties[key].(map[string]interface{})["type"] = "object"
						properties[key].(map[string]interface{})["properties"] = subProps["properties"]
					case nil:
						properties[key].(map[string]interface{})["type"] = "null"
					case string:
						if _, err := time.Parse("2006-01-02 15:04:05.999", value.(string)); err == nil {
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

func generateState(record Record, responseRecordsPath string, updatedAtField string) {
	if responseRecordsPath == "default" {
		responseRecordsPath = "results"
	}
	if updatedAtField == "" {
		updatedAtField = "updated_at"
	}
	stream := make(map[string]interface{})
	data := make(map[string]interface{})

	if _, err := os.Stat("state.json"); os.IsNotExist(err) {
		data["updated_at"] = record[updatedAtField].(string)
		stream[responseRecordsPath] = data

		values := make(map[string]interface{})
		values["bookmarks"] = stream

		message := Message{
			Type:          "STATE",
			Value:         values,
			TimeExtracted: time.Now(),
		}

		messageJson, err := json.Marshal(message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating STATE message: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(messageJson))
		os.WriteFile("state.json", messageJson, 0644)

	} else {
		var state map[string]interface{}
		bytes, _ := os.ReadFile("state.json")
		_ = json.Unmarshal(bytes, &state)

		if record[updatedAtField].(string) > state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[responseRecordsPath].(map[string]interface{})["updated_at"].(string) {
			data["updated_at"] = record[updatedAtField].(string)
			stream[responseRecordsPath] = data

			values := make(map[string]interface{})
			values["bookmarks"] = stream

			message := Message{
				Type:          "STATE",
				Value:         values,
				TimeExtracted: time.Now(),
			}

			messageJson, err := json.Marshal(message)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating STATE message: %v\n", err)
				os.Exit(1)
			}
			fmt.Println(string(messageJson))
			os.WriteFile("state.json", messageJson, 0644)
		}
	}
}

func parseResponse(url, responseRecordsPath string, idField string, updatedAtField string) {
	if responseRecordsPath == "" {
		responseRecordsPath = "default"
	}
	if responseRecordsPath == "" {
		responseRecordsPath = "id"
	}

	apiResponse, err := http.Get(url)
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

	/////////////////////////////////////////////////////////////
	// Parse API response JSON into a Map
	/////////////////////////////////////////////////////////////
	var responseMap map[string]interface{}
	if responseRecordsPath == "default" && output[0:1] == "{" {
		outputAsArray := "{\"results\":[" + output + "]}"
		jsonParseErr := json.Unmarshal([]byte(outputAsArray), &responseMap)
		if jsonParseErr != nil {
			fmt.Fprintf(os.Stderr, "Error parsing JSON Array (refer to responseRecordsPath) 1: %v\n", err)
			os.Exit(1)
		}
	} else if responseRecordsPath == "default" && output[0:1] == "[" {
		outputAsArray := "{\"results\":" + output + "}"
		jsonParseErr := json.Unmarshal([]byte(outputAsArray), &responseMap)
		if jsonParseErr != nil {
			fmt.Fprintf(os.Stderr, "Error parsing JSON Array (refer to responseRecordsPath) 1: %v\n", err)
			os.Exit(1)
		}
	} else {
		jsonParseErr := json.Unmarshal([]byte(output), &responseMap)
		if jsonParseErr != nil {
			fmt.Fprintf(os.Stderr, "Error parsing JSON Array (refer to responseRecordsPath) 2: %v\n", err)
			os.Exit(1)
		}
	}

	records, ok := responseMap["results"].([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	/////////////////////////////////////////////////////////////
	// OUTPUT SCHEMA message
	/////////////////////////////////////////////////////////////
	schemaMessage := Message{
		Type:               "SCHEMA",
		Stream:             responseRecordsPath,
		Schema:             generateSchema(records),
		KeyProperties:      []string{idField},
		BookmarkProperties: []string{"updated_at"},
		TimeExtracted:      time.Now(),
	}
	schemaJson, err := json.Marshal(schemaMessage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating SCHEMA message: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(schemaJson))

	/////////////////////////////////////////////////////////////
	// OUTPUT RECORD messages & UPDATE STATE Message
	/////////////////////////////////////////////////////////////
	for _, record := range records {
		Record, ok := record.(map[string]interface{})
		if !ok {
			fmt.Fprint(os.Stderr, "Error: user is not a map\n")
			os.Exit(1)
		}

		message := Message{
			Type:          "RECORD",
			Data:          Record,
			Stream:        responseRecordsPath,
			TimeExtracted: time.Now(),
		}
		messageJson, err := json.Marshal(message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating RECORD message: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(messageJson))
		if updatedAtField != "" {
			generateState(Record, responseRecordsPath, updatedAtField)
		}
	}

}

func main() {

	// https://rickandmortyapi.com/api/character/2, "", "id", "created"
	// https://rickandmortyapi.com/api/character/, "results", "id", "created"
	// https://cat-fact.herokuapp.com/facts, "", "_id", "updatedAt"

	parseResponse(
		// url
		"https://cat-fact.herokuapp.com/facts",
		// responseRecordsPath
		"",
		// ID
		"_id",
		// bookmark
		"updatedAt",
	)

}
