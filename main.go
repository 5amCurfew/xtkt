package main

import (
	"encoding/json"
	"fmt"
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

func generateState(record Record, streamName string, updatedAtField string) {
	if updatedAtField == "" {
		updatedAtField = "updated_at"
	}
	stream := make(map[string]interface{})
	data := make(map[string]interface{})

	if _, err := os.Stat("state.json"); os.IsNotExist(err) {
		data["updated_at"] = record[updatedAtField].(string)
		stream[streamName] = data

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

		if record["updatedAt"].(string) > state["value"].(map[string]interface{})["bookmarks"].(map[string]interface{})[streamName].(map[string]interface{})["updated_at"].(string) {
			data["updated_at"] = record[updatedAtField].(string)
			stream[streamName] = data

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

func main() {

	/////////////////////////////////////////////////////////////
	// EXAMPLE
	/////////////////////////////////////////////////////////////
	responseRecordsPath := "data"

	apiResponse := `
	{
		"data": [{
			"type": "articles",
			"id": "1",
			"attributes": {
			"title": "JSON:API paints my bikeshed!",
			"body": "The shortest article. Ever.",
			"created": "2015-05-22T14:56:29.000Z",
			"updated": "2015-05-22T14:56:28.000Z"
			},
			"relationships": {
			"author": {
				"data": {"id": "42", "type": "people"}
			}
			},
			"test": true,
			"updatedAt": "2020-01-02 15:04:05.999",
			"createdAt": "2020-01-02"
		},
		{
			"type": "articles",
			"id": "2",
			"attributes": {
				"title": "JSON:API paints my bikeshed!",
				"body": "The shortest article. Ever.",
				"created": "2015-05-22T14:56:29.000Z",
				"updated": "2015-05-22T14:56:28.000Z"
			},
			"relationships": {
				"author": {
				"data": {"id": "42", "type": "people"}
				}
			},
			"test": true,
			"updatedAt": "2020-01-02 17:04:05.999",
			"createdAt": "2020-01-02"
		}],
		"included": [
		  {
			"type": "people",
			"id": "42",
			"attributes": {
			  "name": "John",
			  "age": 80,
			  "gender": "male"
			}
		  }
		]
	  }
    `

	/////////////////////////////////////////////////////////////
	// Parse API response JSON into a Map
	/////////////////////////////////////////////////////////////
	var responseMap map[string]interface{}
	err := json.Unmarshal([]byte(apiResponse), &responseMap)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	records, ok := responseMap[responseRecordsPath].([]interface{})
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
		KeyProperties:      []string{"id"},
		BookmarkProperties: []string{"updated_at"},
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

		generateState(Record, responseRecordsPath, "updatedAt")
	}
}
