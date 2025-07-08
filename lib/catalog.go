package lib

import (
	"encoding/json"
	"fmt"
	"os"

	util "github.com/5amCurfew/xtkt/util"
)

var DerivedCatalog *StreamCatalog

type StreamCatalog struct {
	KeyProperties []string               `json:"key_properties"`
	Schema        map[string]interface{} `json:"schema"`
	Stream        string                 `json:"stream"`
}

// Create <STREAM>_catalog.json
func CreateCatalogJSON() error {
	if ParsedConfig.StreamName == nil {
		return fmt.Errorf("error creating catalog file stream name is nil")
	}

	streamName := *ParsedConfig.StreamName // Ensure ParsedConfig.StreamName is initialized

	c := StreamCatalog{
		KeyProperties: []string{"_sdc_surrogate_key"},
		Schema:        map[string]interface{}{}, // Initialize as an empty map
		Stream:        streamName,
	}

	// Write JSON file
	fileName := fmt.Sprintf("%s_catalog.json", streamName)
	err := util.WriteJSON(fileName, c)
	if err != nil {
		return fmt.Errorf("error writing catalog.json: %v", err)
	}

	return nil
}

// Parse <STREAM>_catalog.json
func ReadCatalogJSON() (*StreamCatalog, error) {
	catalogFile, err := os.ReadFile(fmt.Sprintf("%s_catalog.json", *ParsedConfig.StreamName))
	if err != nil {
		return nil, fmt.Errorf("error reading catalog file: %w", err)
	}

	var catalog StreamCatalog
	if err := json.Unmarshal(catalogFile, &catalog); err != nil {
		return nil, fmt.Errorf("error unmarshaling catalog json: %w", err)
	}

	return &catalog, nil
}

func UpdateCatalogJSON() {
	util.WriteJSON(fmt.Sprintf("%s_catalog.json", *ParsedConfig.StreamName), DerivedCatalog)
}
