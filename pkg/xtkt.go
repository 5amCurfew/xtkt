package xtkt

import (
	"os"

	lib "github.com/5amCurfew/xtkt/lib"
	util "github.com/5amCurfew/xtkt/util"
)

func ParseResponse(config util.Config) {
	/////////////////////////////////////////////////////////////
	// GENERATE RECORDS
	/////////////////////////////////////////////////////////////
	var records []interface{}

	if *config.SourceType == "rest" {
		records = lib.GenerateRestRecords(config)
	} else if *config.SourceType == "database" {
		records = lib.GenerateDatabaseRecords(config)
	}

	lib.AddMetadata(records)

	/////////////////////////////////////////////////////////////
	// GENERATE BOOKMARK (if required)
	/////////////////////////////////////////////////////////////
	if lib.IsBookmarkProvided(config) {
		if _, err := os.Stat("state.json"); os.IsNotExist(err) {
			lib.CreateBookmark(config)
		}
	}

	/////////////////////////////////////////////////////////////
	// GENERATE SCHEMA Message
	/////////////////////////////////////////////////////////////
	lib.GenerateSchemaMessage(records, config)

	/////////////////////////////////////////////////////////////
	// GENERATE RECORD Message(s)
	/////////////////////////////////////////////////////////////
	lib.GenerateRecordMessages(records, config)

	/////////////////////////////////////////////////////////////
	// GENERATE STATE Message (if required) given RECORDS
	/////////////////////////////////////////////////////////////
	if lib.IsBookmarkProvided(config) {
		if lib.IsRecordDetectionProvided(config) {
			lib.UpdateDetectionBookmark(records, config)
		} else {
			lib.UpdateBookmark(records, config)
		}
		lib.GenerateStateMessage()
	}

}
