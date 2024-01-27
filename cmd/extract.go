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
	NewRecords        uint64        `json:"new_records"`
}

// /////////////////////////////////////////////////////////
// EXTRACT
// /////////////////////////////////////////////////////////
func extract(saveSchema bool) error {
	var execution ExecutionMetric
	execution.Stream = *lib.ParsedConfig.StreamName
	execution.ExecutionStart = time.Now().UTC()

	// /////////////////////////////////////////////////////////
	// GENERATE state_<STREAM>.json
	// /////////////////////////////////////////////////////////
	if _, err := os.Stat(fmt.Sprintf("state_%s.json", *lib.ParsedConfig.StreamName)); err != nil {
		lib.CreateStateJSON()
	}

	// /////////////////////////////////////////////////////////
	// PARSE CURRENT STATE
	// /////////////////////////////////////////////////////////
	state, parseStateError := lib.ParseStateJSON()
	if parseStateError != nil {
		return fmt.Errorf("error PARSING STATE JSON %w", parseStateError)
	}
	lib.ParsedState = state

	// /////////////////////////////////////////////////////////
	// GENERATE RECORDS
	// /////////////////////////////////////////////////////////
	records, generateRecordsError := generateRecords()
	if generateRecordsError != nil {
		return fmt.Errorf("error CREATING RECORDS: %w", generateRecordsError)
	}

	// /////////////////////////////////////////////////////////
	// GENERATE SCHEMA, SCHEMA MESSAGE
	// /////////////////////////////////////////////////////////
	if len(records) > 0 {
		schema, generateSchemaError := lib.GenerateSchema(records)
		if generateSchemaError != nil {
			return fmt.Errorf("error GENERATING SCHEMA: %w", generateSchemaError)
		}
		if generateSchemaMessageError := lib.GenerateSchemaMessage(schema); generateSchemaMessageError != nil {
			return fmt.Errorf("error GENERATING SCHEMA MESSAGE: %w", generateSchemaMessageError)
		}
		if saveSchema {
			util.WriteJSON(fmt.Sprintf("schema_%s.json", time.Now().Format("20060102150405")), schema)
		}
	}

	// /////////////////////////////////////////////////////////
	// GENERATE RECORD MESSAGES
	// /////////////////////////////////////////////////////////
	for _, record := range records {
		if generateRecordMessageError := lib.GenerateRecordMessage(record); generateRecordMessageError != nil {
			return fmt.Errorf("error GENERATING RECORD MESSAGE: %w", generateRecordMessageError)
		}
		lib.UpdateState(record)
	}

	// /////////////////////////////////////////////////////////
	// UPDATE STATE (& state_<STREAM>.json)
	// /////////////////////////////////////////////////////////
	log.Info(fmt.Sprintf(`state json updated at %s`, time.Now().UTC().Format(time.RFC3339)))

	// /////////////////////////////////////////////////////////
	// GENERATE STATE MESSAGE
	// /////////////////////////////////////////////////////////
	if generateStateMessageError := lib.GenerateStateMessage(state); generateStateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", generateStateMessageError)
	}

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	if len(records) > 0 {
		execution.NewRecords = uint64(len(records))
	} else {
		execution.NewRecords = uint64(0)
	}
	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}

// /////////////////////////////////////////////////////////
// GENERATE RECORDS
// /////////////////////////////////////////////////////////
func generateRecords() ([]interface{}, error) {
	switch *lib.ParsedConfig.SourceType {
	case "db":
		log.Info(fmt.Sprintf(`generating records from database %s`, strings.Split(*lib.ParsedConfig.URL, "@")[0]))
		return sources.GatherRecords(sources.ParseDB)
	case "csv":
		log.Info(fmt.Sprintf(`generating records from file at %s`, *lib.ParsedConfig.URL))
		return sources.GatherRecords(sources.ParseCSV)
	case "jsonl":
		log.Info(fmt.Sprintf(`generating records from file at %s`, *lib.ParsedConfig.URL))
		return sources.GatherRecords(sources.ParseJSONL)
	case "rest":
		log.Info(fmt.Sprintf(`generating records from REST-API %s`, *lib.ParsedConfig.URL))
		return sources.GatherRecords(sources.ParseREST)
	default:
		return nil, fmt.Errorf("unsupported data source in GenerateRecords")
	}
}
