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

		// _sdc_surrogate_key, _sdc_unique_key
		if key == "_sdc_surrogate_key" || key == "_sdc_unique_key" {
			prop["type"] = "string"
			prop["minLength"] = 1
			properties[key] = prop
			continue
		}

		// _sdc_natural_key: required and non-nullable
		if key == "_sdc_natural_key" {
			switch v := value.(type) {
			case string:
				prop["type"] = "string"
			case bool:
				prop["type"] = "boolean"
			case int, int32, int64, float32, float64:
				prop["type"] = "number"
			case map[string]interface{}:
				subSchema, err := GenerateSchema(v)
				if err != nil {
					return nil, fmt.Errorf("error schema generation recursion: %w", err)
				}
				prop["type"] = "object"
				prop["properties"] = subSchema["properties"]
			case []interface{}:
				prop["type"] = "array"
			default:
				prop["type"] = "string"
			}
			properties[key] = prop
			continue
		}

		// General case for all other fields
		switch v := value.(type) {
		case bool:
			prop["type"] = []string{"boolean", "null"}
		case int, int32, int64, float32, float64:
			prop["type"] = []string{"number", "null"}
		case map[string]interface{}:
			subSchema, err := GenerateSchema(v)
			if err != nil {
				return nil, fmt.Errorf("error schema generation recursion: %w", err)
			}
			prop["type"] = []string{"object", "null"}
			prop["properties"] = subSchema["properties"]
		case []interface{}:
			prop["type"] = []string{"array", "null"}
		case nil:
			continue
		case string:
			if _, err := time.Parse(time.RFC3339, v); err == nil {
				prop["type"] = []string{"string", "null"}
				prop["format"] = "date-time"
			} else if _, err := time.Parse("2006-01-02", v); err == nil {
				prop["type"] = []string{"string", "null"}
				prop["format"] = "date"
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
	existingSchema["type"] = []string{"object", "null"}

	return existingSchema, nil
}
