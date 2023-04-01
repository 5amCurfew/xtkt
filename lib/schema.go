package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

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

func GenerateSchemaMessage(records []interface{}, c Config) {
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
}
