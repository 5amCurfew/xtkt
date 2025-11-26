package lib

import (
	"encoding/json"
	"runtime"
	"sync"

	"github.com/5amCurfew/xtkt/models"
	log "github.com/sirupsen/logrus"
)

var ExtractedChan = make(chan map[string]interface{})
var ResultChan = make(chan map[string]interface{}, 100)
var ProcessingWG sync.WaitGroup
var workerSem = make(chan struct{}, runtime.NumCPU()) // Limit concurrent workers to number of CPUs

// TransformationMetrics tracks record transformation statistics
type TransformationMetrics struct {
	Processed uint64 `json:"processed"`
	Skipped   uint64 `json:"skipped"`
	Filtered  uint64 `json:"filtered"` // filtered by bookmark
	mu        sync.Mutex
}

var TransformMetrics = &TransformationMetrics{}

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
		workerSem <- struct{}{} // Acquire semaphore
		go extractRecord(record)
	}
}

// extractRecord processes a record (sending to the ResultChan)
func extractRecord(record map[string]interface{}) {
	defer ProcessingWG.Done()
	defer func() { <-workerSem }() // Release semaphore

	TransformMetrics.mu.Lock()
	TransformMetrics.Processed += 1
	TransformMetrics.mu.Unlock()

	if processedData, err := transformRecord(record); err == nil && processedData != nil {
		ResultChan <- processedData
	} else if err != nil {
		recordWithError, _ := json.Marshal(record)
		log.WithFields(log.Fields{
			"record": json.RawMessage(recordWithError), // logs as nested JSON, no escaping
			"error":  err,
		}).Warn("error processing record - skipping...")

		TransformMetrics.mu.Lock()
		TransformMetrics.Skipped += 1
		TransformMetrics.mu.Unlock()
	}
}
