package models

import (
	"encoding/json"
	"fmt"
	"os"

	util "github.com/5amCurfew/xtkt/util"
	"github.com/xeipuuv/gojsonschema"
)

var DerivedCatalog StreamCatalog

type StreamCatalog struct {
	KeyProperties []string               `json:"key_properties"`
	Schema        map[string]interface{} `json:"schema"`
	Stream        string                 `json:"stream"`
}

// Create <STREAM>_catalog.json
func (c *StreamCatalog) Create() error {

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

// RecordVersusCatalog validates record against Catalog
func (c *StreamCatalog) RecordVersusCatalog(record map[string]interface{}) (bool, error) {
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
	message := Message{
		Type:          "SCHEMA",
		Stream:        c.Stream,
		Schema:        c.Schema,
		KeyProperties: c.KeyProperties,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING SCHEMA MESSAGE: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))

	return nil
}
