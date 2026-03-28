package cmd

import (
	"fmt"
	"time"

	"github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
	"github.com/5amCurfew/xtkt/sources"
	log "github.com/sirupsen/logrus"
)

type ExecutionMetric struct {
	ExecutionStart    time.Time     `json:"execution_start,omitempty"`
	ExecutionEnd      time.Time     `json:"execution_end,omitempty"`
	ExecutionDuration time.Duration `json:"execution_duration,omitempty"`
	Processed         uint64        `json:"processed"`
	Emitted           uint64        `json:"emitted"`
	Skipped           uint64        `json:"skipped"`
	PerSecond         float64       `json:"per_second"`
	Filtered          uint64        `json:"filtered"`
}

// Root function for extracting data from source
func Extract(discover bool, refresh bool) error {
	var execution ExecutionMetric
	execution.ExecutionStart = time.Now().UTC()

	// initialise state and catalog files
	if err := models.State.Create(); err != nil {
		return fmt.Errorf("error initialising state: %w", err)
	}

	// Mark the start of this extraction run
	models.State.StartExtraction()

	if err := models.DerivedCatalog.Create(); err != nil {
		return fmt.Errorf("error initialising catalog: %w", err)
	}

	models.FULL_REFRESH = refresh

	// Start the record extraction stream
	startRecordStream()

	// Run in discovery mode or extraction mode
	if discover {
		if err := runDiscoveryMode(); err != nil {
			return err
		}
	} else {
		if err := processRecords(&execution); err != nil {
			return err
		}
	}

	// Finalize extraction and log metrics
	return finaliseExtraction(&execution)
}

// startRecordStream initiates the goroutine to extract and transform records
func startRecordStream() {
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
}

// runDiscoveryMode runs catalog discovery and validates the schema
func runDiscoveryMode() error {
	discoverCatalog()

	if len(models.DerivedCatalog.Schema) == 0 {
		return fmt.Errorf("error gathering schema from source")
	}

	if err := models.DerivedCatalog.Message(); err != nil {
		return fmt.Errorf("error generating schema message: %w", err)
	}

	return nil
}

// processRecords processes records from the stream and validates against catalog
func processRecords(execution *ExecutionMetric) error {
	if len(models.DerivedCatalog.Schema) == 0 {
		return fmt.Errorf("error gathering schema from catalog - ensure the catalog exists by running xtkt <CONFIG> --discover")
	}

	if err := models.DerivedCatalog.Message(); err != nil {
		return fmt.Errorf("error generating schema message: %w", err)
	}

	for record := range lib.ResultChan {
		if valid, err := models.DerivedCatalog.ValidateRecordAgainstCatalog(record); !valid {
			log.WithFields(log.Fields{
				"_sdc_natural_key": record["_sdc_natural_key"],
				"error":            err,
			}).Warn("record violates schema constraints in catalog - skipping...")

			execution.Skipped += 1
			continue
		}

		var rec models.Record
		if err := rec.Create(record); err != nil {
			return fmt.Errorf("error creating record: %w", err)
		}

		if err := rec.Message(); err != nil {
			return fmt.Errorf("error generating record message: %w", err)
		}

		// Note: UpdateBookmark is now called earlier in lib/extract.go
		// to ensure last_seen is updated for all records (including unchanged)

		execution.Emitted += 1
	}

	return nil
}

// finaliseExtraction writes state, calculates metrics, and logs results
func finaliseExtraction(execution *ExecutionMetric) error {
	if err := models.State.Update(); err != nil {
		return fmt.Errorf("error writing state: %w", err)
	}

	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)

	// Add transformation metrics
	execution.Processed = lib.TransformMetrics.Processed
	execution.Filtered = lib.TransformMetrics.Filtered

	// Calculate records per second based on records processed
	if execution.ExecutionDuration.Seconds() > 0 {
		execution.PerSecond = float64(execution.Processed) / execution.ExecutionDuration.Seconds()
	}

	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}
