package cmd

import (
	"fmt"
	"os"
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
	sources "github.com/5amCurfew/xtkt/sources"
	"github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

type ExecutionMetric struct {
	ExecutionStart    time.Time     `json:"execution_start,omitempty"`
	ExecutionEnd      time.Time     `json:"execution_end,omitempty"`
	ExecutionDuration time.Duration `json:"execution_duration,omitempty"`
	NewRecords        uint64        `json:"new_records"`
}

// Root function for extracting data from source, requires disccover flag
func Extract(discover bool) error {
	var execution ExecutionMetric
	execution.ExecutionStart = time.Now().UTC()

	streamName := *lib.ParsedConfig.StreamName

	// Create state.json
	if _, err := os.Stat(fmt.Sprintf("%s_state.json", streamName)); err != nil {
		err := lib.CreateStateJSON()
		if err != nil {
			return fmt.Errorf("error creating state.json: %w", err)
		}
	}

	// Create catalog.json
	if _, err := os.Stat(fmt.Sprintf("%s_catalog.json", streamName)); err != nil {
		err := lib.CreateCatalogJSON()
		if err != nil {
			return fmt.Errorf("error creating catalog.json: %w", err)
		}
	}

	// Read current state
	state, err := lib.ReadStateJSON()
	if err != nil {
		return fmt.Errorf("error reading state.json %w", err)
	}
	lib.ParsedState = state

	// Read latest catalog
	catalog, err := lib.ReadCatalogJSON()
	if err != nil {
		return fmt.Errorf("error reading catalog.json %w", err)
	}
	lib.DerivedCatalog = catalog

	// Initiate goroutine to begin extracting and processing records
	go func() {
		defer close(sources.ResultChan)
		log.Info(fmt.Sprintf(`generating records from %s`, *lib.ParsedConfig.URL))

		switch *lib.ParsedConfig.SourceType {
		case "csv":
			sources.ExtractRecords(sources.StreamCSVRecords)
		case "jsonl":
			sources.ExtractRecords(sources.StreamJSONLRecords)
		case "rest":
			sources.ExtractRecords(sources.StreamRESTRecords)
		default:
			log.Info("unsupported data source")
		}

		sources.ProcessingWG.Wait()
	}()

	// Run in discovery mode to create the catalog by listening for extracted records on ResultsChan
	if discover {
		discoverCatalog()

		schema := lib.DerivedCatalog.Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from source")
		}

		if produceSchemaMessageError := lib.ProduceSchemaMessage(schema); produceSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", produceSchemaMessageError)
		}
	}

	// If the catalog exists, begin listening for extracted records on ResultsChan
	if !discover {

		schema := lib.DerivedCatalog.Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from catalog - ensure the catalog exists by running xtkt <CONFIG> --discover")
		}

		if produceSchemaMessageError := lib.ProduceSchemaMessage(schema); produceSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", produceSchemaMessageError)
		}

		for record := range sources.ResultChan {
			if valid, validateRecordSchemaError := lib.ValidateRecordSchema(record, schema); !valid {
				log.WithFields(log.Fields{
					"_sdc_natural_key": record["_sdc_natural_key"],
					"error":            validateRecordSchemaError,
				}).Warn("record violates schema constraints in catalog")
			}

			if produceRecordMessageError := lib.ProduceRecordMessage(record); produceRecordMessageError != nil {
				return fmt.Errorf("error generating record message: %w", produceRecordMessageError)
			}

			lib.UpdateState(record)
			execution.NewRecords += 1
		}
	}

	util.WriteJSON(fmt.Sprintf("%s_state.json", *lib.ParsedConfig.StreamName), lib.ParsedState)

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}

// infers the catalog by listening for all processed records on ResultsChan
func discoverCatalog() {
	for record := range sources.ResultChan {
		recordSchema, _ := lib.GenerateSchema(record)
		existingSchema := lib.DerivedCatalog.Schema

		properties, _ := lib.UpdateSchema(existingSchema, recordSchema)
		lib.DerivedCatalog.Schema = properties
	}

	lib.UpdateCatalogJSON()
}
