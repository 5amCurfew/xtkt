package lib

import (
	"fmt"
	"time"
)

// GenerateSchema generates a JSON schema from a record
func GenerateSchema(record interface{}) (map[string]interface{}, error) {
	schema := make(map[string]interface{})
	properties := make(map[string]interface{})

	r, ok := record.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("error parsing record as map[string]interface{} in GenerateSchema")
	}

	for key, value := range r {
		prop := make(map[string]interface{})
		switch v := value.(type) {
		case bool:
			prop["type"] = []string{"boolean", "null"}
		case int, int32, int64, float32, float64:
			prop["type"] = []string{"number", "null"}
		case map[string]interface{}:
			// Recursive call for nested objects
			subSchema, err := GenerateSchema(v)
			if err != nil {
				return nil, fmt.Errorf("error schema generation recursion: %w", err)
			}
			prop["type"] = []string{"object", "null"}
			prop["properties"] = subSchema["properties"]
		case []interface{}:
			// Type array doesn't require `properties`
			prop["type"] = []string{"array", "null"}
		case nil:
			// Skip null fields; wait for a first non-null value
			continue
		case string:
			if _, err := time.Parse(time.RFC3339, v); err == nil {
				prop["type"] = []string{"string", "null"}
				prop["format"] = "date-time"
			} else if _, err := time.Parse("2006-01-02", v); err == nil {
				prop["type"] = []string{"string", "null"}
				prop["format"] = "date"
			} else if key == "_sdc_surrogate_key" || key == "_sdc_natural_key" {
				// Required and non-empty
				prop["type"] = "string" // non-nullable
				prop["minLength"] = 1   // must be non-empty
			} else {
				prop["type"] = []string{"string", "null"}
			}
		default:
			prop["type"] = []string{"string", "null"}
		}
		properties[key] = prop
	}

	schema["properties"] = properties
	schema["type"] = "object"

	return schema, nil
}

// UpdateSchema merges the new schema into the existing schema
func UpdateSchema(existingSchema, newSchema map[string]interface{}) (map[string]interface{}, error) {
	if existingSchema == nil {
		existingSchema = make(map[string]interface{})
	}

	// Ensure "properties" exists in the existing schema
	properties, ok := existingSchema["properties"].(map[string]interface{})
	if !ok {
		properties = make(map[string]interface{})
		existingSchema["properties"] = properties
	}

	// Extract "properties" from the new schema
	newProperties, ok := newSchema["properties"].(map[string]interface{})
	if !ok {
		return existingSchema, nil
	}

	// Iterate through new properties and merge them into the existing schema
	for key, newValue := range newProperties {
		if existingValue, exists := properties[key]; exists {
			// If both values are objects, merge them recursively
			existingValueMap, existingIsMap := existingValue.(map[string]interface{})
			newValueMap, newIsMap := newValue.(map[string]interface{})
			if existingIsMap && newIsMap {
				// Recursive call for nested objects
				mergedValue, err := UpdateSchema(existingValueMap, newValueMap)
				if err != nil {
					return nil, err
				}
				properties[key] = mergedValue
			}
		} else {
			properties[key] = newValue
		}
	}

	existingSchema["properties"] = properties
	existingSchema["type"] = "object"

	return existingSchema, nil
}
