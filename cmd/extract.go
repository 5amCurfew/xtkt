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

	// Create state.json
	if _, err := os.Stat(fmt.Sprintf("%s_state.json", *lib.ParsedConfig.StreamName)); err != nil {
		lib.CreateStateJSON()
	}

	// Create catalog.json
	if _, err := os.Stat(fmt.Sprintf("%s_catalog.json", *lib.ParsedConfig.StreamName)); err != nil {
		lib.CreateCatalogJSON()
	}

	// Read current state
	state, parseStateError := lib.ReadStateJSON()
	if parseStateError != nil {
		return fmt.Errorf("error parsing state.json %w", parseStateError)
	}
	lib.ParsedState = state

	// Read latest catalog
	catalog, parseCatalogError := lib.ReadCatalogJSON()
	if parseCatalogError != nil {
		return fmt.Errorf("error parsing catalog.json %w", parseCatalogError)
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

	// Run in discovery mode to create the catalog by listening for parsed records on ResultsChan
	if discover {
		discoverCatalog()

		schema := lib.DerivedCatalog.Streams[0].Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from source")
		}

		if generateSchemaMessageError := lib.GenerateSchemaMessage(schema); generateSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", generateSchemaMessageError)
		}
	}

	// If the catalog exists, begin listening for parsed records on ResultsChan
	if !discover {

		schema := lib.DerivedCatalog.Streams[0].Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from catalog - ensure the catalog exists by running xtkt <CONFIG> --discover")
		}

		if generateSchemaMessageError := lib.GenerateSchemaMessage(schema); generateSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", generateSchemaMessageError)
		}

		for record := range sources.ResultChan {
			if valid, validateRecordSchemaError := lib.ValidateRecordSchema(record, schema); !valid {
				log.WithFields(log.Fields{
					"_sdc_natural_key": record["_sdc_natural_key"],
					"error":            validateRecordSchemaError,
				}).Warn("record breaks schema in catalog")
			}

			if generateRecordMessageError := lib.GenerateRecordMessage(record); generateRecordMessageError != nil {
				return fmt.Errorf("error generating record message: %w", generateRecordMessageError)
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
		existingSchema := lib.DerivedCatalog.Streams[0].Schema

		properties, _ := lib.UpdateSchema(existingSchema, recordSchema)
		lib.DerivedCatalog.Streams[0].Schema = properties
	}

	lib.UpdateCatalogJSON()
}
