package lib

import (
	"fmt"
	"time"
)

// ///////////////////////////////////////////////////////////
// GENERATE JSON SCHEMA
// ///////////////////////////////////////////////////////////
func GenerateSchema(records []interface{}) (map[string]interface{}, error) {
	schema := make(map[string]interface{})
	properties := make(map[string]interface{})

	for _, record := range records {
		r, err := record.(map[string]interface{})
		if !err {
			return nil, fmt.Errorf("error SCHEMA CREATION FROM RECORD")
		}

		for key, value := range r {
			prop, exists := properties[key]
			if !exists {
				prop = make(map[string]interface{})
				properties[key] = prop
			}

			switch value.(type) {
			case bool:
				prop.(map[string]interface{})["type"] = []string{"boolean", "null"}
			case int:
				prop.(map[string]interface{})["type"] = []string{"integer", "null"}
			case float64:
				prop.(map[string]interface{})["type"] = []string{"number", "null"}
			case map[string]interface{}:
				if subProps, err := GenerateSchema([]interface{}{value}); err == nil {
					prop.(map[string]interface{})["type"] = []string{"object", "null"}
					prop.(map[string]interface{})["properties"] = subProps["properties"]
				} else {
					return nil, fmt.Errorf("error SCHEMA RECURSION: %w", err)
				}
			case []interface{}:
				prop.(map[string]interface{})["type"] = []string{"array", "null"}
			case nil:
				continue // wait for first non-null value for field
			case string:
				if _, err := time.Parse(time.RFC3339, value.(string)); err == nil {
					prop.(map[string]interface{})["type"] = []string{"string", "null"}
					prop.(map[string]interface{})["format"] = "date-time"
				} else if _, err := time.Parse("2006-01-02", value.(string)); err == nil {
					prop.(map[string]interface{})["type"] = []string{"string", "null"}
					prop.(map[string]interface{})["format"] = "date"
				} else {
					prop.(map[string]interface{})["type"] = []string{"string", "null"}
				}
			default:
				prop.(map[string]interface{})["type"] = []string{"string", "null"}
			}
		}
	}

	schema["properties"] = properties
	schema["type"] = "object"
	return schema, nil
}
