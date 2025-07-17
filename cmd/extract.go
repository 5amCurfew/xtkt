package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
	"github.com/5amCurfew/xtkt/sources"
	"github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

type ExecutionMetric struct {
	ExecutionStart    time.Time     `json:"execution_start,omitempty"`
	ExecutionEnd      time.Time     `json:"execution_end,omitempty"`
	ExecutionDuration time.Duration `json:"execution_duration,omitempty"`
	Records           uint64        `json:"records"`
	Skipped           uint64        `json:"skipped"`
}

// Root function for extracting data from source
func Extract(discover bool, refresh bool) error {
	var execution ExecutionMetric
	execution.ExecutionStart = time.Now().UTC()

	// Create state.json
	if _, err := os.Stat(fmt.Sprintf("%s_state.json", models.STREAM_NAME)); err != nil {
		err := models.State.Create()
		if err != nil {
			return fmt.Errorf("error creating state file: %w", err)
		}
	}

	// Create catalog.json
	if _, err := os.Stat(fmt.Sprintf("%s_catalog.json", models.STREAM_NAME)); err != nil {
		err := models.DerivedCatalog.Create()
		if err != nil {
			return fmt.Errorf("error creating catalog file: %w", err)
		}
	}

	models.FULL_REFRESH = refresh

	// Read latest state
	stateErr := models.State.Read()
	if stateErr != nil {
		return fmt.Errorf("error reading state %w", stateErr)
	}

	// Read latest catalog
	catalogErr := models.DerivedCatalog.Read()
	if catalogErr != nil {
		return fmt.Errorf("error reading catalog %w", catalogErr)
	}

	// Initiate goroutine to begin extraction and transformation of records
	go func() {
		defer close(lib.ResultChan)
		log.Info(fmt.Sprintf(`generating records from %s`, models.Config.URL))

		switch models.Config.SourceType {
		case "csv":
			lib.ExtractRecords(sources.StreamCSVRecords)
		case "jsonl":
			lib.ExtractRecords(sources.StreamJSONLRecords)
		case "rest":
			lib.ExtractRecords(sources.StreamRESTRecords)
		default:
			log.Info("unsupported data source")
		}

		lib.ProcessingWG.Wait()
	}()

	// Run in discovery mode to create the catalog by listening for records on ResultsChan
	if discover {
		discoverCatalog()

		schema := models.DerivedCatalog.Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from source")
		}

		if produceSchemaMessageError := models.DerivedCatalog.Message(); produceSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", produceSchemaMessageError)
		}
	}

	// If the catalog exists, begin listening for records on ResultsChan
	if !discover {

		schema := models.DerivedCatalog.Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from catalog - ensure the catalog exists by running xtkt <CONFIG> --discover")
		}

		if produceSchemaMessageError := models.DerivedCatalog.Message(); produceSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", produceSchemaMessageError)
		}

		for record := range lib.ResultChan {
			if valid, validateRecordSchemaError := models.DerivedCatalog.RecordVersusCatalog(record); !valid {
				log.WithFields(log.Fields{
					"_sdc_natural_key": record["_sdc_natural_key"],
					"error":            validateRecordSchemaError,
				}).Warn("record violates schema constraints in catalog - skipping...")

				execution.Skipped += 1
				continue
			}

			if produceRecordMessageError := lib.RecordMessage(record); produceRecordMessageError != nil {
				return fmt.Errorf("error generating record message: %w", produceRecordMessageError)
			}

			models.State.Update(record)
			execution.Records += 1
		}
	}

	util.WriteJSON(fmt.Sprintf("%s_state.json", models.STREAM_NAME), models.State)

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}
