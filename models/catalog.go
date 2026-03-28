package models

import (
	"encoding/json"
	"fmt"
	"os"

	util "github.com/5amCurfew/xtkt/util"
	"github.com/xeipuuv/gojsonschema"
)

// Compile-time verification that StreamCatalog implements Model interface
var _ Model = (*StreamCatalog)(nil)

// StreamCatalog represents a stream's schema catalog and implements the Model interface.
// It manages the JSON schema definition, key properties, and provides validation
// capabilities for records against the catalog schema.
type StreamCatalog struct {
	KeyProperties []string               `json:"key_properties"`
	Schema        map[string]interface{} `json:"schema"`
	Stream        string                 `json:"stream"`
}

var DerivedCatalog StreamCatalog

// Create creates a catalog JSON file for the stream
func (c *StreamCatalog) Create(source ...interface{}) error {
	// Check if file already exists
	if _, err := os.Stat(fmt.Sprintf("%s_catalog.json", STREAM_NAME)); err == nil {
		// File exists, read it instead of creating new
		return c.Read()
	}

	c.Stream = STREAM_NAME
	if c.Stream == "" {
		return fmt.Errorf("error creating catalog file: stream name is required")
	}
	c.KeyProperties = []string{"_sdc_unique_key", "_sdc_surrogate_key"}
	c.Schema = map[string]interface{}{}

	fileName := fmt.Sprintf("%s_catalog.json", c.Stream)
	err := util.WriteJSON(fileName, c)
	if err != nil {
		return fmt.Errorf("error writing catalog.json: %v", err)
	}

	return nil
}

// Read the Catalog JSON file
func (c *StreamCatalog) Read() error {
	catalogFile, err := os.ReadFile(fmt.Sprintf("%s_catalog.json", STREAM_NAME))
	if err != nil {
		return fmt.Errorf("error reading catalog file: %w", err)
	}

	if err := json.Unmarshal(catalogFile, c); err != nil {
		return fmt.Errorf("error unmarshaling catalog json: %w", err)
	}

	return nil
}

// Update the Catalog JSON file
func (c *StreamCatalog) Update() error {
	fileName := fmt.Sprintf("%s_catalog.json", c.Stream)
	err := util.WriteJSON(fileName, c)
	if err != nil {
		return fmt.Errorf("error updating catalog.json: %v", err)
	}
	return nil
}

// ValidateRecordAgainstCatalog validates record against Catalog
func (c *StreamCatalog) ValidateRecordAgainstCatalog(record map[string]interface{}) (bool, error) {
	schemaLoader := gojsonschema.NewGoLoader(c.Schema)
	recordLoader := gojsonschema.NewGoLoader(record)

	result, _ := gojsonschema.Validate(schemaLoader, recordLoader)

	if result.Valid() {
		return true, nil
	}

	return false, fmt.Errorf("%s", result.Errors())
}

// Message generates a schema message from the derived catalog
func (c *StreamCatalog) Message() error {
	var schema Schema
	schema.Create(c.Schema)
	return schema.Message()
}
