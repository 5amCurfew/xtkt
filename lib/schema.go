package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	util "github.com/5amCurfew/xtkt/util"
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
						properties[key].(map[string]interface{})["type"] = []string{"boolean", "null"}
					case int:
						properties[key].(map[string]interface{})["type"] = []string{"integer", "null"}
					case float64:
						properties[key].(map[string]interface{})["type"] = []string{"number", "null"}
					case map[string]interface{}:
						subProps := generateSchema([]interface{}{value})
						properties[key].(map[string]interface{})["type"] = []string{"object", "null"}
						properties[key].(map[string]interface{})["properties"] = subProps["properties"]
					case []interface{}:
						properties[key].(map[string]interface{})["type"] = []string{"array", "null"}
					case nil:
						properties[key].(map[string]interface{})["type"] = []string{"null", "null"}
					case string:
						if _, err := time.Parse(time.RFC3339, value.(string)); err == nil {
							properties[key].(map[string]interface{})["type"] = []string{"string", "null"}
							properties[key].(map[string]interface{})["format"] = "date-time"
							break
						} else if _, err := time.Parse("2006-01-02", value.(string)); err == nil {
							properties[key].(map[string]interface{})["type"] = []string{"string", "null"}
							properties[key].(map[string]interface{})["format"] = "date"
							break
						} else {
							properties[key].(map[string]interface{})["type"] = []string{"string", "null"}
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

func GenerateSchemaMessage(records []interface{}, config util.Config) {
	message := util.Message{
		Type:          "SCHEMA",
		Stream:        util.GenerateStreamName(URLsParsed[0], config),
		TimeExtracted: time.Now().Format(time.RFC3339),
		Schema:        generateSchema(records),
		KeyProperties: []string{"surrogate_key"},
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating SCHEMA message: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(messageJson))
}
