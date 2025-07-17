package lib

import (
	"encoding/json"
	"sync"

	"github.com/5amCurfew/xtkt/models"
	log "github.com/sirupsen/logrus"
)

var ExtractedChan = make(chan map[string]interface{})
var ResultChan = make(chan map[string]interface{})
var ProcessingWG sync.WaitGroup

// ExtractRecords begins streaming records from source (sending to ExtractedChan) and start goroutines to extract records
func ExtractRecords(streamFunc func(*models.StreamConfig) error) {
	// begin a goroutine to stream records from source
	go func() {
		defer close(ExtractedChan)
		if err := streamFunc(&models.Config); err != nil {
			log.WithFields(log.Fields{"error": err}).Info("ProcessRecords: stream function failed")
		}
	}()

	// begin a goroutine for each extracted record, processing the record (sending to the ResultChan)
	for record := range ExtractedChan {
		ProcessingWG.Add(1)
		go extractRecord(record)
	}
}

// extractRecord processes a record (sending to the ResultChan)
func extractRecord(record map[string]interface{}) {
	defer ProcessingWG.Done()

	if processedData, err := transformRecord(record); err == nil && processedData != nil {
		ResultChan <- processedData
	} else if err != nil {
		recordWithError, _ := json.Marshal(record)
		log.WithFields(log.Fields{
			"record": json.RawMessage(recordWithError), // logs as nested JSON, no escaping
			"error":  err,
		}).Warn("error processing record - skipping...")
	}
}
