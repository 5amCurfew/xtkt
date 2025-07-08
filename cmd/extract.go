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
	ExecutionStart       time.Time     `json:"execution_start,omitempty"`
	ExecutionEnd         time.Time     `json:"execution_end,omitempty"`
	ExecutionDuration    time.Duration `json:"execution_duration,omitempty"`
	NewRecords           uint64        `json:"new_records"`
	NewQuarantineRecords uint64        `json:"new_quarantine_records"`
}

// Root function for extracting data from source
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
		defer close(lib.ResultChan)
		log.Info(fmt.Sprintf(`generating records from %s`, *lib.ParsedConfig.URL))

		switch *lib.ParsedConfig.SourceType {
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

		for record := range lib.ResultChan {
			if valid, validateRecordSchemaError := lib.ValidateRecordSchema(record, schema); !valid {
				log.WithFields(log.Fields{
					"_sdc_natural_key": record["_sdc_natural_key"],
					"error":            validateRecordSchemaError,
				}).Warn("record violates schema constraints in catalog - record _sdc_natural_key added to quarantine")

				lib.UpdateStateQuarantine(record)
				execution.NewQuarantineRecords += 1
				continue
			}

			if produceRecordMessageError := lib.ProduceRecordMessage(record); produceRecordMessageError != nil {
				return fmt.Errorf("error generating record message: %w", produceRecordMessageError)
			}

			lib.UpdateStateBookmark(record)
			execution.NewRecords += 1
		}
	}

	util.WriteJSON(fmt.Sprintf("%s_state.json", *lib.ParsedConfig.StreamName), lib.ParsedState)

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)
	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}
