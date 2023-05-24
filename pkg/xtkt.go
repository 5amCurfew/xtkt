package xtkt

import (
	"fmt"
	"os"
	"reflect"
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

func Extract(config lib.Config) error {
	// GENERATE STATE.JSON
	if _, err := os.Stat("state.json"); err != nil {
		CreateStateJSONError := lib.CreateStateJSON(config)
		if CreateStateJSONError != nil {
			return fmt.Errorf("error CREATING STATE.JSON: %w", CreateStateJSONError)
		}
	}

	// PARSE CURRENT STATE
	state, parseStateError := lib.ParseStateJSON(config)
	if parseStateError != nil {
		return fmt.Errorf("error parsing state ParseStateJSON() %w", parseStateError)
	}

	// RECORDS
	var records []interface{}
	var err error
	switch *config.SourceType {
	case "rest":
		records, err = lib.GenerateRestRecords(config)
	case "db":
		records, err = lib.GenerateDatabaseRecords(config)
	case "html":
		records, err = lib.GenerateHtmlRecords(config)
	}
	if err != nil {
		return fmt.Errorf("error CREATING RECORDS: %w", err)
	}

	lib.AddMetadata(records, config)
	if config.Records.SensitivePaths != nil {
		lib.HashRecordsFields(records, config)
	}

	// SCHEMA MESSAGE
	schema, schemaError := lib.GenerateSchema(records)
	if schemaError != nil {
		return fmt.Errorf("error CREATING SCHEMA: %w", schemaError)
	}

	schemaMessageError := lib.GenerateSchemaMessage(schema, config)
	if schemaMessageError != nil {
		return fmt.Errorf("error GENERATING SCHEMA MESSAGE: %w", schemaMessageError)
	}

	// RECORD MESSAGE(S)
	recordCounter := 0
	for _, record := range records {
		recordMessagesError := lib.GenerateRecordMessage(record.(map[string]interface{}), state, config)
		recordCounter++
		if recordMessagesError != nil {
			return fmt.Errorf("error GENERATING RECORD MESSAGE: %w", recordMessagesError)
		}
	}
	log.Info(fmt.Sprintf(`INFO: {type: METRIC, records: %d, completed: %s}`, recordCounter, time.Now().Format(time.RFC3339)))

	// UPDATE STATE
	if lib.UsingBookmark(config) {
		switch path := *config.Records.PrimaryBookmarkPath; {
		case reflect.DeepEqual(path, []string{"*"}):
			UpdateBookmarkError := lib.UpdateBookmarkDetection(records, state, config)
			if UpdateBookmarkError != nil {
				return fmt.Errorf("error UPDATING BOOKMARK (new-record-detection): %w", UpdateBookmarkError)
			}
		default:
			UpdateBookmarkError := lib.UpdateBookmarkPrimary(records, state, config)
			if UpdateBookmarkError != nil {
				return fmt.Errorf("error UPDATING BOOKMARK (primary-bookmark): %w", UpdateBookmarkError)
			}
		}
	}

	lib.UpdateStateUpdatedAt(state, config)

	// UPDATE STATE.JSON
	lib.WriteStateJSON(state)

	// STATE MESSAGE
	stateMessageError := lib.GenerateStateMessage()
	if stateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", stateMessageError)
	}

	return nil
}
