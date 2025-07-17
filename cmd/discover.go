package cmd

import (
	lib "github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
)

// discoverCatalog infers and updates the catalog based on processed records
func discoverCatalog() {
	for record := range lib.ResultChan {
		recordSchema, _ := lib.GenerateSchema(record)
		existingSchema := models.DerivedCatalog.Schema

		properties, _ := lib.UpdateSchema(existingSchema, recordSchema)
		models.DerivedCatalog.Schema = properties
	}

	models.DerivedCatalog.Update()
}
