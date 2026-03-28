package models

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Compile-time verification that Schema implements Model interface
var _ Model = (*Schema)(nil)

// Schema represents a JSON schema with generation and update capabilities.
// It provides methods for working with JSON schemas including property
// management, schema generation from records, and schema merging.
// While similar to Model entities, Schema is an in-memory data structure
// with parameterized methods for flexibility.
type Schema map[string]interface{}

// Create initialises a new Schema, optionally from existing data
func (s *Schema) Create(data ...interface{}) error {
	if len(data) > 0 && data[0] != nil {
		if schemaData, ok := data[0].(map[string]interface{}); ok {
			*s = Schema(schemaData)
		} else {
			return fmt.Errorf("schema data must be map[string]interface{}, got %T", data[0])
		}
	} else {
		*s = make(Schema)
		(*s)["type"] = "object"
		(*s)["properties"] = make(map[string]interface{})
	}
	return nil
}

// Read reads the schema (placeholder for Model interface)
func (s *Schema) Read() error {
	// Schema is loaded via Create method
	return nil
}

// Update updates the schema (placeholder for Model interface)
func (s Schema) Update() error {
	// Schema is immutable once created; use Merge() to combine with records
	return nil
}

// Merge merges this schema with another schema (from a new record)
func (s *Schema) Merge(newRecord map[string]interface{}) error {
	// Generate schema from the new record
	newSchema, err := generateSchemaFromRecord(newRecord)
	if err != nil {
		return fmt.Errorf("error generating schema from record: %w", err)
	}

	// Merge the new schema into this schema
	merged, err := mergeSchemas(s.ToMap(), newSchema)
	if err != nil {
		return fmt.Errorf("error merging schemas: %w", err)
	}

	// Update this schema with merged values
	for k, v := range merged {
		(*s)[k] = v
	}

	return nil
}

// Message generates a SCHEMA type message and writes it to stdout
func (s *Schema) Message() error {
	message := Message{
		Type:          "SCHEMA",
		Stream:        STREAM_NAME,
		Schema:        s.ToMap(),
		KeyProperties: []string{"_sdc_unique_key", "_sdc_surrogate_key"},
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error creating schema message: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))

	return nil
}

// CreateFromRecord generates a schema from a record
func (s *Schema) CreateFromRecord(record interface{}) error {
	schema, err := generateSchemaFromRecord(record)
	if err != nil {
		return err
	}
	*s = Schema(schema)
	return nil
}

// Properties returns the properties of the schema
func (s Schema) Properties() map[string]interface{} {
	if props, ok := s["properties"].(map[string]interface{}); ok {
		return props
	}
	return make(map[string]interface{})
}

// SetProperties sets the properties of the schema
func (s Schema) SetProperties(properties map[string]interface{}) {
	s["properties"] = properties
	s["type"] = "object"
}

// ToMap converts the Schema back to a plain map
func (s Schema) ToMap() map[string]interface{} {
	return map[string]interface{}(s)
}

// IsEmpty returns true if the schema has no properties
func (s Schema) IsEmpty() bool {
	return len(s) == 0 || len(s.Properties()) == 0
}

// generateSchemaFromRecord generates a JSON schema from a record (internal)
func generateSchemaFromRecord(record interface{}) (map[string]interface{}, error) {
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
				subSchema, err := generateSchemaFromRecord(v)
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
			subSchema, err := generateSchemaFromRecord(v)
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

// mergeSchemas merges newSchema into existingSchema (internal)
func mergeSchemas(existingSchema, newSchema map[string]interface{}) (map[string]interface{}, error) {
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
				mergedValue, err := mergeSchemas(existingValueMap, newValueMap)
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
