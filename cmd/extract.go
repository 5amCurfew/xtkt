package cmd

import (
	"fmt"

	"github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
	"github.com/5amCurfew/xtkt/sources"
	log "github.com/sirupsen/logrus"
)

// Extract runs the default extraction flow.
func Extract(refresh bool) error {
	execution := lib.NewExecutionMetric()
	if err := initialiseRun(false, refresh); err != nil {
		return err
	}

	if err := ensureCatalogSchemaAvailable(); err != nil {
		return err
	}

	startRecordStream()

	if err := processRecords(&execution); err != nil {
		return err
	}

	return finaliseExtraction(&execution)
}

func logAndWrapError(message string, err error, fields log.Fields) error {
	entry := log.WithField("error", err)
	if len(fields) > 0 {
		entry = entry.WithFields(fields)
	}
	entry.Error(message)
	return fmt.Errorf("%s: %w", message, err)
}

func logAndReturnError(message string, fields log.Fields) error {
	entry := log.NewEntry(log.StandardLogger())
	if len(fields) > 0 {
		entry = entry.WithFields(fields)
	}
	entry.Error(message)
	return fmt.Errorf(message)
}

func initialiseRun(discover bool, refresh bool) error {
	models.FULL_REFRESH = refresh
	models.DISCOVER_MODE = discover

	// initialise state and catalog files
	if err := models.State.Create(); err != nil {
		return logAndWrapError("state initialisation failed", err, log.Fields{
			"discover": discover,
			"refresh":  refresh,
		})
	}

	// Mark the start of this extraction run
	models.State.StartExtraction()

	if err := models.DerivedCatalog.Create(); err != nil {
		return logAndWrapError("catalog initialisation failed", err, log.Fields{
			"discover": discover,
			"refresh":  refresh,
		})
	}

	return nil
}

func ensureCatalogSchemaAvailable() error {
	if len(models.DerivedCatalog.Schema) == 0 {
		return logAndReturnError("catalog schema unavailable; run discovery first", nil)
	}

	return nil
}

// startRecordStream initiates the goroutine to extract and transform records
func startRecordStream() {
	go func() {
		defer close(lib.ResultChan)
		log.WithFields(log.Fields{
			"source_type": models.Config.SourceType,
			"url":         models.Config.URL,
		}).Info("starting record extraction")

		switch models.Config.SourceType {
		case "csv":
			lib.ExtractRecords(sources.StreamCSVRecords)
		case "jsonl":
			lib.ExtractRecords(sources.StreamJSONLRecords)
		case "rest":
			lib.ExtractRecords(sources.StreamRESTRecords)
		default:
			log.WithField("source_type", models.Config.SourceType).Warn("unsupported source type")
		}

		lib.ProcessingWG.Wait()
	}()
}

// processRecords processes records from the stream and validates against catalog
func processRecords(execution *lib.ExecutionMetric) error {
	if err := ensureCatalogSchemaAvailable(); err != nil {
		return err
	}

	if err := models.DerivedCatalog.Message(); err != nil {
		return logAndWrapError("schema message generation failed", err, nil)
	}

	for record := range lib.ResultChan {
		if valid, err := models.DerivedCatalog.ValidateRecordAgainstCatalog(record); !valid {
			log.WithFields(log.Fields{
				"_sdc_natural_key": record["_sdc_natural_key"],
				"error":            err,
			}).Warn("record failed schema validation; not emitting")

			execution.NotEmitted.SchemaValidationFailed += 1
			continue
		}

		var rec models.Record
		if err := rec.Create(record); err != nil {
			return logAndWrapError("record creation failed", err, log.Fields{
				"record": record,
			})
		}

		if err := rec.Message(); err != nil {
			return logAndWrapError("record message generation failed", err, log.Fields{
				"_sdc_natural_key": rec["_sdc_natural_key"],
			})
		}

		// Only records that pass schema validation should advance state.
		if !models.DISCOVER_MODE {
			models.State.UpdateBookmark(rec.ToMap())
		}

		execution.Emitted += 1
	}

	return nil
}

// finaliseExtraction writes state, calculates metrics, and logs results
func finaliseExtraction(execution *lib.ExecutionMetric) error {
	if err := models.State.Update(); err != nil {
		return logAndWrapError("state update failed", err, nil)
	}

	execution.Complete()

	log.WithFields(log.Fields{"metrics": execution}).Info("execution metrics")
	return nil
}
