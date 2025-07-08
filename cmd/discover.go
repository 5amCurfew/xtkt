package cmd

import (
	lib "github.com/5amCurfew/xtkt/lib"
)

// infers the catalog by listening for all processed records on ResultsChan
func discoverCatalog() {
	for record := range lib.ResultChan {
		recordSchema, _ := lib.GenerateSchema(record)
		existingSchema := lib.DerivedCatalog.Schema

		properties, _ := lib.UpdateSchema(existingSchema, recordSchema)
		lib.DerivedCatalog.Schema = properties
	}

	lib.UpdateCatalogJSON()
}
