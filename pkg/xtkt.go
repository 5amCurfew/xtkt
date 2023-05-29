package xtkt

import (
	"fmt"
	"os"
	"strings"
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
		return fmt.Errorf("error PARSING STATE.JSON %w", parseStateError)
	}

	// RECORDS
	var records []interface{}
	var generateRecordsError error
	switch *config.SourceType {
	case "db":
		log.Info(fmt.Sprintf(`INFO: generating records from database %s`, strings.Split(*config.URL, "@")[0]))
		records, generateRecordsError = lib.GenerateDatabaseRecords(config)
	case "file":
		log.Info(fmt.Sprintf(`INFO: generating records from file at %s`, *config.URL))
		records, generateRecordsError = lib.GenerateFileRecords(config)
	case "html":
		log.Info(fmt.Sprintf(`INFO: generating records from HTML page %s`, *config.URL))
		records, generateRecordsError = lib.GenerateHtmlRecords(config)
	case "rest":
		log.Info(fmt.Sprintf(`INFO: generating records from REST-api %s`, *config.URL))
		records, generateRecordsError = lib.GenerateRestRecords(config)
	}
	if generateRecordsError != nil {
		return fmt.Errorf("error CREATING RECORDS: %w", generateRecordsError)
	}
	log.Info(fmt.Sprintf(`INFO: records generated at %s}`, time.Now().UTC().Format(time.RFC3339)))

	// PROCESS RECORDS
	processRecordsError := lib.ProcessRecords(&records, config)
	if processRecordsError != nil {
		return fmt.Errorf("error PROCESSING RECORDS: %w", processRecordsError)
	}

	// SCHEMA MESSAGE
	schema, generateSchemaError := lib.GenerateSchema(records)
	if generateSchemaError != nil {
		return fmt.Errorf("error CREATING SCHEMA: %w", generateSchemaError)
	}

	generateSchemaMessageError := lib.GenerateSchemaMessage(schema, config)
	if generateSchemaMessageError != nil {
		return fmt.Errorf("error GENERATING SCHEMA MESSAGE: %w", generateSchemaMessageError)
	}

	// RECORD MESSAGE(S)
	recordCounter := 0
	for _, record := range records {
		generateRecordMessageError := lib.GenerateRecordMessage(record, state, config)
		if generateRecordMessageError != nil {
			return fmt.Errorf("error GENERATING RECORD MESSAGE: %w", generateRecordMessageError)
		}
		recordCounter++
	}
	log.Info(fmt.Sprintf(`INFO: {type: METRIC, records: %d, completed: %s}`, recordCounter, time.Now().UTC().Format(time.RFC3339)))

	// UPDATE STATE & STATE.JSON
	updateStateError := lib.UpdateState(records, state, config)
	if updateStateError != nil {
		return fmt.Errorf("error UPDATING STATE: %w", updateStateError)
	}
	log.Info(fmt.Sprintf(`INFO: state.json updated at %s}`, time.Now().UTC().Format(time.RFC3339)))

	// STATE MESSAGE
	generateStateMessageError := lib.GenerateStateMessage(state)
	if generateStateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", generateStateMessageError)
	}

	return nil
}
