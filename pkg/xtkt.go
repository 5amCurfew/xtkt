package xtkt

import (
	"os"

	lib "github.com/5amCurfew/xtkt/lib"
	util "github.com/5amCurfew/xtkt/util"
)

func bookmarkSet(c util.Config) bool {
	return c.Bookmark && c.PrimaryBookmark != ""
}

func ParseResponse(config util.Config) {

	records := lib.GenerateRecords(config)

	/////////////////////////////////////////////////////////////
	// GENERATE BOOKMARK (if required)
	/////////////////////////////////////////////////////////////
	if bookmarkSet(config) {
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
	if bookmarkSet(config) {
		lib.UpdateBookmark(records, config)
		lib.GenerateStateMessage()
	}

}
