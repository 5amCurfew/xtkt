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

// Extract: root function for extracting data from source, requires disccover flag
func extract(discover bool) error {
	var execution ExecutionMetric
	execution.ExecutionStart = time.Now().UTC()

	// Create <STREAM>_state.json
	if _, err := os.Stat(fmt.Sprintf("%s_state.json", *lib.ParsedConfig.StreamName)); err != nil {
		lib.CreateStateJSON()
	}

	// Create <STREAM>_catalog.json
	if _, err := os.Stat(fmt.Sprintf("%s_catalog.json", *lib.ParsedConfig.StreamName)); err != nil {
		lib.CreateCatalogJSON()
	}

	// Parse current state
	state, parseStateError := lib.ParseStateJSON()
	if parseStateError != nil {
		return fmt.Errorf("error parsing state.json %w", parseStateError)
	}
	lib.ParsedState = state

	// Parse latest catalog
	catalog, parseCatalogError := lib.ParseCatalogJSON()
	if parseCatalogError != nil {
		return fmt.Errorf("error parsing catalog.json %w", parseCatalogError)
	}
	lib.ParsedCatalog = catalog

	// Initiate goroutine to begin extracting and parsing records
	go func() {
		log.Info(fmt.Sprintf(`generating records from %s`, *lib.ParsedConfig.URL))

		switch *lib.ParsedConfig.SourceType {
		case "csv":
			sources.ParseRecords(sources.StreamCSVRecords)
		case "jsonl":
			sources.ParseRecords(sources.StreamJSONLRecords)
		case "rest":
			sources.ParseRecords(sources.StreamRESTRecords)
		default:
			log.Info("unsupported data source")
		}

		sources.ParsingWG.Wait()
		close(sources.ResultChan)
	}()

	// Run in discovery mode to create the catalog by listening for parsed records on ResultsChan
	if discover {
		discoverCatalog()

		schema := lib.ParsedCatalog.Streams[0].Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from source")
		}

		if generateSchemaMessageError := lib.GenerateSchemaMessage(schema); generateSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", generateSchemaMessageError)
		}
	}

	// If the catalog exists, begin listening for parsed records on ResultsChan
	if !discover {

		schema := lib.ParsedCatalog.Streams[0].Schema
		if len(schema) == 0 {
			return fmt.Errorf("error gathering schema from catalog - ensure the catalog exists by running xtkt <CONFIG> --discover")
		}

		if generateSchemaMessageError := lib.GenerateSchemaMessage(schema); generateSchemaMessageError != nil {
			return fmt.Errorf("error generating schema message: %w", generateSchemaMessageError)
		}

		for record := range sources.ResultChan {
			r := *record
			rMap, _ := r.(map[string]interface{})
			if valid, validateRecordSchemaError := lib.ValidateRecordSchema(rMap, schema); !valid {
				log.WithFields(log.Fields{
					"_sdc_natural_key": rMap["_sdc_natural_key"],
					"error":            validateRecordSchemaError,
				}).Warn("record breaks schema in catalog")
			}

			if generateRecordMessageError := lib.GenerateRecordMessage(r); generateRecordMessageError != nil {
				return fmt.Errorf("error generating record message: %w", generateRecordMessageError)
			}

			lib.UpdateStateBookmark(r)
			execution.NewRecords += 1
		}
	}

	util.WriteJSON(fmt.Sprintf("%s_state.json", *lib.ParsedConfig.StreamName), lib.ParsedState)

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}

// infers the catalog by listening for all parsed records on ResultsChan
func discoverCatalog() {
	for record := range sources.ResultChan {
		r := *record
		recordSchema, _ := lib.GenerateSchema(r)
		existingSchema := lib.ParsedCatalog.Streams[0].Schema

		properties, _ := lib.UpdateSchema(existingSchema, recordSchema)
		lib.ParsedCatalog.Streams[0].Schema = properties
	}

	lib.UpdateCatalogJSON()
}
