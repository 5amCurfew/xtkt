package cmd

import (
	lib "github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// Discover runs catalog discovery mode.
func Discover(refresh bool) error {
	execution := lib.NewExecutionMetric()
	if err := initialiseRun(true, refresh); err != nil {
		return err
	}

	startRecordStream()

	if err := runDiscoveryMode(); err != nil {
		return err
	}

	return finaliseExtraction(&execution)
}

// runDiscoveryMode runs catalog discovery and validates the schema
func runDiscoveryMode() error {
	if err := discoverCatalog(); err != nil {
		return err
	}

	if len(models.DerivedCatalog.Schema) == 0 {
		return logAndReturnError("discovery produced an empty schema", nil)
	}

	if err := models.DerivedCatalog.Message(); err != nil {
		return logAndWrapError("discovery schema message generation failed", err, nil)
	}

	return nil
}

// discoverCatalog infers and updates the catalog based on processed records
func discoverCatalog() error {
	var catalogSchema models.Schema
	if err := catalogSchema.Create(models.DerivedCatalog.Schema); err != nil {
		return logAndWrapError("discovery schema initialisation failed", err, nil)
	}

	for record := range lib.ResultChan {
		// Update the schema with the new record
		if err := catalogSchema.Merge(record.ToMap()); err != nil {
			log.WithFields(log.Fields{
				"_sdc_natural_key": record["_sdc_natural_key"],
				"error":            err,
			}).Warn("record schema merge failed during discovery")
			continue
		}
	}

	// Update the catalog's schema with the merged schema
	models.DerivedCatalog.Schema = catalogSchema.ToMap()
	models.DerivedCatalog.SchemaDiscoveredAt = util.NowTimestamp()
	if err := models.DerivedCatalog.Update(); err != nil {
		return logAndWrapError("derived catalog update failed", err, nil)
	}

	return nil
}
