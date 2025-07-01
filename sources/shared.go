package sources

import (
	"fmt"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

var ExtractedChan = make(chan map[string]interface{})
var ResultChan = make(chan map[string]interface{})
var ProcessingWG sync.WaitGroup

// Begin streaming records from source (and sending to ExtractedChan) and start goroutines to process records
func ExtractRecords(streamFunc func(lib.Config) error) {
	// begin a goroutine to stream records from source
	go func() {
		defer close(ExtractedChan)
		if err := streamFunc(lib.ParsedConfig); err != nil {
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

	if processedData, err := lib.ProcessRecord(record); err == nil && processedData != nil {
		ResultChan <- processedData
	} else if err != nil {
		log.Warn(fmt.Sprintf("error processing record %s: %v", record, err))
	}
}
