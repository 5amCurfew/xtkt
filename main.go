package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Record map[string]interface{}

type Message struct {
	Type               string      `json:"type"`
	Data               Record      `json:"data,omitempty"`
	Stream             string      `json:"stream,omitempty"`
	Schema             interface{} `json:"schema,omitempty"`
	KeyProperties      []string    `json:"key_properties,omitempty"`
	BookmarkProperties []string    `json:"bookmark_properties,omitempty"`
}

func main() {

	apiResponseRecordsPath := "users"

	apiResponse := `
    {
        "users": [
            {
                "id": 1,
                "name": "John",
                "email": "john@example.com",
				"details": {
					"location": "London, UK"
				} 
            },
            {
                "id": 2,
                "name": "Jane",
                "email": "jane@example.com"
            }
        ]
    }
    `

	// Parse API response JSON into a map
	var responseMap map[string]interface{}
	err := json.Unmarshal([]byte(apiResponse), &responseMap)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	// Output schema message for the response
	schema := make(map[string]interface{})
	properties := make(map[string]interface{})
	records, ok := responseMap[apiResponseRecordsPath].([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}
	if len(records) > 0 {
		records, ok := records[0].(map[string]interface{})
		if !ok {
			fmt.Fprint(os.Stderr, "Error: record is not a dictionary\n")
			os.Exit(1)
		}
		for key, value := range records {
			properties[key] = make(map[string]string)
			properties[key].(map[string]string)["type"] = "string"
			switch value.(type) {
			case int:
				properties[key].(map[string]string)["type"] = "integer"
			case float64:
				properties[key].(map[string]string)["type"] = "number"
			}
		}
	}

	schema["properties"] = properties
	schema["type"] = "object"
	keyProperties := []string{"id"}
	schemaMessage := Message{
		Type:               "SCHEMA",
		Stream:             apiResponseRecordsPath,
		Schema:             schema,
		KeyProperties:      keyProperties,
		BookmarkProperties: []string{"updated_at"},
	}
	schemaJson, err := json.Marshal(schemaMessage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating Singer schema message: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(schemaJson))

	// Create a Singer message for each record in the response
	for _, record := range records {
		Record, ok := record.(map[string]interface{})
		if !ok {
			fmt.Fprint(os.Stderr, "Error: user is not a dictionary\n")
			os.Exit(1)
		}
		message := Message{
			Type:   "RECORD",
			Data:   Record,
			Stream: apiResponseRecordsPath,
		}
		messageJson, err := json.Marshal(message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating Singer message: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(messageJson))
	}
}
