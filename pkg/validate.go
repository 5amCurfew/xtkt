package xtkt

import (
	"encoding/json"
	"fmt"

	lib "github.com/5amCurfew/xtkt/lib"
)

func ValidateJSONConfig(jsonBytes []byte) error {
	var cfg lib.Config
	err := json.Unmarshal(jsonBytes, &cfg)
	if err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}
	if cfg.StreamName == nil {
		return fmt.Errorf("missing required field: StreamName string")
	}
	if cfg.SourceType == nil {
		return fmt.Errorf("missing required field: SourceType string")
	}
	return nil
}
