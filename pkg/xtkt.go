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

	if jsonError := json.Unmarshal(config, &cfg); jsonError != nil {
		return cfg, fmt.Errorf("error parseConfigJson unmarshlling config.json: %w", jsonError)
	}

	return cfg, nil
}

func generateRecords(config lib.Config) ([]interface{}, error) {
	switch *config.SourceType {
	case "db":
		log.Info(fmt.Sprintf(`generating records from database %s`, strings.Split(*config.URL, "@")[0]))
		return sources.GenerateDatabaseRecords(config)
	case "file":
		log.Info(fmt.Sprintf(`generating records from file at %s`, *config.URL))
		return sources.GenerateFileRecords(config)
	case "html":
		log.Info(fmt.Sprintf(`generating records from HTML page %s`, *config.URL))
		return sources.GenerateHtmlRecords(config)
	case "rest":
		log.Info(fmt.Sprintf(`generating records from REST-api %s`, *config.URL))
		return sources.GenerateRestRecords(config)
	default:
		return nil, fmt.Errorf("unsupported data source in generateRecords")
	}
}

func Extract(config lib.Config) error {
	var execution lib.ExecutionMetric
	execution.Stream = *config.StreamName
	execution.ExecutionStart = time.Now().UTC()

	// /////////////////////////////////////////////////////////
	// GENERATSE state.json
	// /////////////////////////////////////////////////////////
	if _, err := os.Stat("state.json"); err != nil {
		lib.CreateStateJSON(config)
	}

	// /////////////////////////////////////////////////////////
	// PARSE CURRENT STATE
	// /////////////////////////////////////////////////////////
	state, parseStateError := lib.ParseStateJSON(config)
	if parseStateError != nil {
		return fmt.Errorf("error PARSING STATE.JSON %w", parseStateError)
	}

	// /////////////////////////////////////////////////////////
	// GENERATSE RECORDS
	// /////////////////////////////////////////////////////////
	records, generateRecordsError := generateRecords(config)
	if generateRecordsError != nil {
		return fmt.Errorf("error CREATING RECORDS: %w", generateRecordsError)
	}
	execution.RecordsExtracted = len(records)
	log.Info(fmt.Sprintf(`%d records extracted at %s`, len(records), time.Now().UTC().Format(time.RFC3339)))

	// /////////////////////////////////////////////////////////
	// PROCESS RECORDS
	// /////////////////////////////////////////////////////////
	if processRecordsError := lib.ProcessRecords(&records, state, config); processRecordsError != nil {
		execution.RecordsProcessed = len(records)
		return fmt.Errorf("error PROCESSING RECORDS: %w", processRecordsError)
	}

	// /////////////////////////////////////////////////////////
	// GENERATE & PROCESS SCHEMA
	// /////////////////////////////////////////////////////////
	if processSchemaError := lib.ProcessSchema(records, config); processSchemaError != nil {
		return fmt.Errorf("error PROCESSING SCHEMA: %w", processSchemaError)
	}

	// /////////////////////////////////////////////////////////
	// GENERATSE RECORD MESSAGES
	// /////////////////////////////////////////////////////////
	for _, record := range records {
		if generateRecordMessageError := lib.GenerateRecordMessage(record, state, config); generateRecordMessageError != nil {
			return fmt.Errorf("error GENERATING RECORD MESSAGE: %w", generateRecordMessageError)
		}
	}

	// /////////////////////////////////////////////////////////
	// UPDATE STATE (& state.json)
	// /////////////////////////////////////////////////////////
	lib.UpdateState(records, state, config)
	log.Info(fmt.Sprintf(`state.json updated at %s`, time.Now().UTC().Format(time.RFC3339)))

	// /////////////////////////////////////////////////////////
	// GENERATSE STATE MESSAGE
	// /////////////////////////////////////////////////////////
	if generateStateMessageError := lib.GenerateStateMessage(state); generateStateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", generateStateMessageError)
	}

	// /////////////////////////////////////////////////////////
	// UPDATE history.json
	// /////////////////////////////////////////////////////////
	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	if appendToHistoryError := lib.AppendToHistory(execution); appendToHistoryError != nil {
		return fmt.Errorf("error GENERATING APPENDING EXECUTION TO HISTORY: %w", appendToHistoryError)
	}

	return nil
}

func Listen(config lib.Config) {
	sources.StartListening(config)
}
