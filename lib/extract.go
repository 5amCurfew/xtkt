package lib

import (
	"encoding/json"
	"sync"

	log "github.com/sirupsen/logrus"
)

var ExtractedChan = make(chan map[string]interface{})
var ResultChan = make(chan map[string]interface{})
var ProcessingWG sync.WaitGroup

// Begin streaming records from source (and sending to ExtractedChan) and start goroutines to process records
func ExtractRecords(streamFunc func(Config) error) {
	// begin a goroutine to stream records from source
	go func() {
		defer close(ExtractedChan)
		if err := streamFunc(ParsedConfig); err != nil {
			log.WithFields(log.Fields{"error": err}).Info("ExtractRecords: stream function failed")
		}
	}()

	// begin a goroutine for each extracted record, processing the record (and sending to the ResultChan)
	for record := range ExtractedChan {
		ProcessingWG.Add(1)
		go process(record)
	}
}

// Process a record (and send to ResultChan)
func process(record map[string]interface{}) {
	defer ProcessingWG.Done()

	if processedData, err := ProcessRecord(record); err == nil && processedData != nil {
		ResultChan <- processedData
	} else if err != nil {
		recordWithError, _ := json.Marshal(record)
		log.WithFields(log.Fields{
			"record": json.RawMessage(recordWithError), // logs as nested JSON, no escaping
			"error":  err,
		}).Warn("error processing record")
	}
}
