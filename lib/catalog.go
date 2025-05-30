package lib

import (
	"encoding/json"
	"fmt"
	"os"

	util "github.com/5amCurfew/xtkt/util"
)

var DerivedCatalog *Catalog

type Catalog struct {
	Streams []StreamCatalog `json:"streams"`
}

type StreamCatalog struct {
	Stream        string                 `json:"stream"`
	TapStreamID   string                 `json:"tap_stream_id"`
	KeyProperties []string               `json:"key_properties"`
	Schema        map[string]interface{} `json:"schema"`
}

// Create <STREAM>_catalog.json
func CreateCatalogJSON() {
	if ParsedConfig.StreamName == nil {
		fmt.Println("Error: ParsedConfig.StreamName is nil")
		return
	}

	streamName := *ParsedConfig.StreamName // Ensure ParsedConfig.StreamName is initialized

	c := Catalog{
		Streams: []StreamCatalog{
			{
				Stream:        streamName,
				TapStreamID:   streamName,
				KeyProperties: []string{"_sdc_surrogate_key"},
				Schema:        map[string]interface{}{}, // Initialize as an empty map
			},
		},
	}

	// Write JSON file
	fileName := fmt.Sprintf("%s_catalog.json", streamName)
	err := util.WriteJSON(fileName, c)
	if err != nil {
		fmt.Printf("Error writing JSON: %v\n", err)
	}
}

// Parse <STREAM>_catalog.json
func ReadCatalogJSON() (*Catalog, error) {
	catalogFile, err := os.ReadFile(fmt.Sprintf("%s_catalog.json", *ParsedConfig.StreamName))
	if err != nil {
		return nil, fmt.Errorf("error reading catalog file: %w", err)
	}

	var catalog Catalog
	if err := json.Unmarshal(catalogFile, &catalog); err != nil {
		return nil, fmt.Errorf("error unmarshaling catalog json: %w", err)
	}

	return &catalog, nil
}

func UpdateCatalogJSON() {
	util.WriteJSON(fmt.Sprintf("%s_catalog.json", *ParsedConfig.StreamName), DerivedCatalog)
}
