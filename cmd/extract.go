package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
	sources "github.com/5amCurfew/xtkt/sources"
	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

type ExecutionMetric struct {
	Stream            string        `json:"stream,omitempty"`
	ExecutionStart    time.Time     `json:"execution_start,omitempty"`
	ExecutionEnd      time.Time     `json:"execution_end,omitempty"`
	ExecutionDuration time.Duration `json:"execution_duration,omitempty"`
}

// /////////////////////////////////////////////////////////
// EXTRACT
// /////////////////////////////////////////////////////////
func extract(config lib.Config, saveSchema bool) error {
	var execution ExecutionMetric
	execution.Stream = *config.StreamName
	execution.ExecutionStart = time.Now().UTC()

	// /////////////////////////////////////////////////////////
	// GENERATE state.json
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
	// GENERATE RECORDS
	// /////////////////////////////////////////////////////////
	records, generateRecordsError := generateRecords(config, state)
	if generateRecordsError != nil {
		return fmt.Errorf("error CREATING RECORDS: %w", generateRecordsError)
	}

	// /////////////////////////////////////////////////////////
	// GENERATE SCHEMA, SCHEMA MESSAGE
	// /////////////////////////////////////////////////////////
	schema, generateSchemaError := lib.GenerateSchema(records)
	if generateSchemaError != nil {
		return fmt.Errorf("error GENERATING SCHEMA: %w", generateSchemaError)
	}
	if generateSchemaMessageError := lib.GenerateSchemaMessage(schema, config); generateSchemaMessageError != nil {
		return fmt.Errorf("error GENERATING SCHEMA MESSAGE: %w", generateSchemaMessageError)
	}
	if saveSchema {
		util.WriteJSON(fmt.Sprintf("schema_%s.json", time.Now().Format("20060102150405")), schema)
	}

	// /////////////////////////////////////////////////////////
	// GENERATE RECORD MESSAGES
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
	// GENERATE STATE MESSAGE
	// /////////////////////////////////////////////////////////
	if generateStateMessageError := lib.GenerateStateMessage(state, config); generateStateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", generateStateMessageError)
	}

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")

	return nil
}

// /////////////////////////////////////////////////////////
// GENERATE RECORDS
// /////////////////////////////////////////////////////////
func generateRecords(config lib.Config, state *lib.State) ([]interface{}, error) {
	switch *config.SourceType {
	case "db":
		log.Info(fmt.Sprintf(`generating records from database %s`, strings.Split(*config.URL, "@")[0]))
		return sources.GenerateDatabaseRecords(config)
	case "csv":
		log.Info(fmt.Sprintf(`generating records from file at %s`, *config.URL))
		return sources.GenerateCSVRecords(config, state)
	case "jsonl":
		log.Info(fmt.Sprintf(`generating records from file at %s`, *config.URL))
		return sources.GenerateJSONLRecords(config, state)
	case "rest":
		log.Info(fmt.Sprintf(`generating records from REST-API %s`, *config.URL))
		return sources.GenerateRESTRecords(config, state)
	default:
		return nil, fmt.Errorf("unsupported data source in GenerateRecords")
	}
}
