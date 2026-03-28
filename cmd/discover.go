package cmd

import (
	lib "github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
)

// discoverCatalog infers and updates the catalog based on processed records
func discoverCatalog() {
	var catalogSchema models.Schema
	catalogSchema.Create(models.DerivedCatalog.Schema)

	for record := range lib.ResultChan {
		// Update the schema with the new record
		if err := catalogSchema.Merge(record); err != nil {
			// Log error but continue processing
			continue
		}
	}

	// Update the catalog's schema with the merged schema
	models.DerivedCatalog.Schema = catalogSchema.ToMap()
	models.DerivedCatalog.Update()
}
