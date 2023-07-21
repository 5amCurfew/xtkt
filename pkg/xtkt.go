package xtkt

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
	sources "github.com/5amCurfew/xtkt/lib/sources"
	log "github.com/sirupsen/logrus"
)

func ParseConfigJSON(filePath string) (lib.Config, error) {
	var cfg lib.Config

	config, readConfigError := os.ReadFile(filePath)
	if readConfigError != nil {
		return cfg, fmt.Errorf("error parseConfigJson reading config.json: %w", readConfigError)
	}

	jsonError := json.Unmarshal(config, &cfg)
	if jsonError != nil {
		return cfg, fmt.Errorf("error parseConfigJson unmarshlling config.json: %w", jsonError)
	}

	return cfg, nil
}

func Extract(config lib.Config) error {
	var execution lib.ExecutionMetric
	execution.Stream = *config.StreamName
	execution.ExecutionStart = time.Now().UTC()

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
		log.Info(fmt.Sprintf(`generating records from database %s`, strings.Split(*config.URL, "@")[0]))
		records, generateRecordsError = sources.GenerateDatabaseRecords(config)
	case "file":
		log.Info(fmt.Sprintf(`generating records from file at %s`, *config.URL))
		records, generateRecordsError = sources.GenerateFileRecords(config)
	case "html":
		log.Info(fmt.Sprintf(`generating records from HTML page %s`, *config.URL))
		records, generateRecordsError = sources.GenerateHtmlRecords(config)
	case "rest":
		log.Info(fmt.Sprintf(`generating records from REST-api %s`, *config.URL))
		records, generateRecordsError = sources.GenerateRestRecords(config)
	}
	if generateRecordsError != nil {
		return fmt.Errorf("error CREATING RECORDS: %w", generateRecordsError)
	}
	execution.RecordsExtracted = len(records)
	log.Info(fmt.Sprintf(`%d records extracted at %s`, len(records), time.Now().UTC().Format(time.RFC3339)))

	// PROCESS RECORDS
	processRecordsError := lib.ProcessRecords(&records, state, config)
	if processRecordsError != nil {
		return fmt.Errorf("error PROCESSING RECORDS: %w", processRecordsError)
	}
	execution.RecordsProcessed = len(records)
	log.Info(fmt.Sprintf(`%d records when processed at %s`, len(records), time.Now().UTC().Format(time.RFC3339)))

	// SCHEMA MESSAGE
	if len(records) > 0 {
		schema, generateSchemaError := lib.GenerateSchema(records)
		if generateSchemaError != nil {
			return fmt.Errorf("error CREATING SCHEMA: %w", generateSchemaError)
		}
		generateSchemaMessageError := lib.GenerateSchemaMessage(schema, config)
		if generateSchemaMessageError != nil {
			return fmt.Errorf("error GENERATING SCHEMA MESSAGE: %w", generateSchemaMessageError)
		}
	}

	// RECORD MESSAGE(S)
	for _, record := range records {
		generateRecordMessageError := lib.GenerateRecordMessage(record, state, config)
		if generateRecordMessageError != nil {
			return fmt.Errorf("error GENERATING RECORD MESSAGE: %w", generateRecordMessageError)
		}
	}

	// UPDATE STATE & STATE.JSON
	updateStateError := lib.UpdateState(records, state, config)
	if updateStateError != nil {
		return fmt.Errorf("error UPDATING STATE: %w", updateStateError)
	}
	log.Info(fmt.Sprintf(`state.json updated at %s`, time.Now().UTC().Format(time.RFC3339)))

	// STATE MESSAGE
	generateStateMessageError := lib.GenerateStateMessage(state)
	if generateStateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", generateStateMessageError)
	}

	// UPDATE HISTORY.JSON
	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	appendToHistoryError := lib.AppendToHistory(execution)
	if appendToHistoryError != nil {
		return fmt.Errorf("error GENERATING APPENDING EXECUTION TO HISTORY: %w", appendToHistoryError)
	}

	return nil
}

func Listen(config lib.Config) {
	sources.StartListening(config)
}
