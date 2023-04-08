package xtkt

import (
	"os"

	lib "github.com/5amCurfew/xtkt/lib"
	util "github.com/5amCurfew/xtkt/util"
)

func ParseResponse(config util.Config) {
	// RECORDS
	var records []interface{}
	switch *config.SourceType {
	case "rest":
		records = lib.GenerateRestRecords(config)
	case "database":
		records, _ = lib.GenerateDatabaseRecords(config)
	}
	lib.AddMetadata(records, config)

	// STATE.JSON (if required)
	if lib.IsBookmarkProvided(config) {
		if _, err := os.Stat("state.json"); err != nil {
			lib.CreateBookmark(config)
		}
	}

	// SCHEMA message
	schema := lib.GenerateSchema(records)
	lib.GenerateSchemaMessage(schema, config)

	// RECORD messages
	for _, record := range records {
		lib.GenerateRecordMessage(record.(map[string]interface{}), config)
	}

	// STATE message (if required)
	if lib.IsBookmarkProvided(config) {
		if lib.IsRecordDetectionProvided(config) {
			lib.UpdateDetectionBookmark(records, config)
		} else {
			lib.UpdateBookmark(records, config)
		}
		lib.GenerateStateMessage()
	}
}
