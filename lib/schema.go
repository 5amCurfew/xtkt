package lib

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Generate Schema message from record
func GenerateSchema(record interface{}) (map[string]interface{}, error) {
	schema := make(map[string]interface{})
	properties := make(map[string]interface{})

	r, err := record.(map[string]interface{})
	if !err {
		return nil, fmt.Errorf("error parsing record as map[string]interface{} in GenerateSchema")
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
			prop.(map[string]interface{})["type"] = []string{"number", "null"}
		case float64:
			prop.(map[string]interface{})["type"] = []string{"number", "null"}
		case map[string]interface{}:
			if subProps, err := GenerateSchema(value); err == nil {
				prop.(map[string]interface{})["type"] = []string{"object", "null"}
				prop.(map[string]interface{})["properties"] = subProps["properties"]
			} else {
				return nil, fmt.Errorf("error schema generation recursion: %w", err)
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
			} else if key == "_sdc_surrogate_key" {
				prop.(map[string]interface{})["type"] = "string"
			} else {
				prop.(map[string]interface{})["type"] = []string{"string", "null"}
			}
		default:
			prop.(map[string]interface{})["type"] = []string{"string", "null"}
		}
	}

	schema["properties"] = properties
	schema["type"] = "object"

	return schema, nil
}

// UpdateSchema merges the new schema into the existing schema
// and initializes "properties" if it doesn't exist.
func UpdateSchema(existingSchema, newSchema map[string]interface{}) (map[string]interface{}, error) {
	if existingSchema == nil {
		existingSchema = make(map[string]interface{})
	}

	// Check if the existing schema has a "properties" field
	properties, ok := existingSchema["properties"].(map[string]interface{})
	if !ok {
		// If "properties" doesn't exist, initialize it as an empty map
		properties = make(map[string]interface{})
		existingSchema["properties"] = properties
	}

	// Extract "properties" from the new schema
	newProperties, ok := newSchema["properties"].(map[string]interface{})
	if !ok {
		log.Warn("newSchema does not contain 'properties' field of type map[string]interface{}")
	}

	// Iterate through new properties and add them to the existing schema if they don't exist already
	for key, value := range newProperties {
		// If the property doesn't exist in the existing schema, add it
		if _, exists := properties[key]; !exists {
			properties[key] = value
		}
	}

	existingSchema["properties"] = properties
	existingSchema["type"] = "object"

	return existingSchema, nil
}
