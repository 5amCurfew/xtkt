package cmd

import (
	"fmt"
	"os"
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
	sources "github.com/5amCurfew/xtkt/sources"
	log "github.com/sirupsen/logrus"
)

type ExecutionMetric struct {
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

	go func() {
		log.Info(fmt.Sprintf(`generating records from %s`, *lib.ParsedConfig.URL))

		switch *lib.ParsedConfig.SourceType {
		case "db":
			sources.ParseDB()
		case "csv":
			sources.ParseCSV()
		case "jsonl":
			sources.ParseJSONL()
		case "rest":
			sources.ParseREST()
		default:
			log.Info("unsupported data source")
		}

		sources.ParsingWG.Wait()
		close(sources.ResultChan)
	}()

	for record := range sources.ResultChan {
		r := *record
		if generateRecordMessageError := lib.GenerateRecordMessage(r); generateRecordMessageError != nil {
			return fmt.Errorf("error GENERATING RECORD MESSAGE: %w", generateRecordMessageError)
		}
		lib.UpdateState(r)
		execution.NewRecords += 1
	}

	// /////////////////////////////////////////////////////////
	// GENERATE STATE MESSAGE
	// /////////////////////////////////////////////////////////
	if generateStateMessageError := lib.GenerateStateMessage(state); generateStateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", generateStateMessageError)
	}

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}
