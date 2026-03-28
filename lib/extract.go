package lib

import (
	"encoding/json"
	"runtime"
	"sync"

	"github.com/5amCurfew/xtkt/models"
	log "github.com/sirupsen/logrus"
)

var ExtractedChan = make(chan map[string]interface{})   // Unbuffered channel for extracted records; processing goroutines will read from this channel
var ResultChan = make(chan map[string]interface{}, 100) // Buffered channel to prevent blocking on writes when processing is slower than extraction
var ProcessingWG sync.WaitGroup                         // WaitGroup to track processing goroutines
var workerSem = make(chan struct{}, runtime.NumCPU())   // Concurrency cap keeps CPU-bound transforms from outnumbering cores

// TransformationMetrics tracks record transformation statistics
type TransformationMetrics struct {
	Processed uint64 `json:"processed"`
	Skipped   uint64 `json:"skipped"`
	Filtered  uint64 `json:"filtered"` // filtered by bookmark
	mu        sync.Mutex
}

var TransformMetrics = &TransformationMetrics{}

// ExtractRecords begins streaming records from source (sending to ExtractedChan) and start goroutines to extract records (sending to ResultChan)
func ExtractRecords(sourceFunc func(*models.StreamConfig) error) {
	// begin a goroutine to stream records from source
	go func() {
		defer close(ExtractedChan)
		if err := sourceFunc(&models.Config); err != nil {
			log.WithFields(log.Fields{"error": err}).Info("ProcessRecords: source function failed")
		}
	}()

	// begin a goroutine for each extracted record, processing the record (sending to the ResultChan)
	for record := range ExtractedChan {
		ProcessingWG.Add(1)
		workerSem <- struct{}{} // Block here when every core already runs a processing goroutine
		go processRecord(record)
	}
}

// processRecord processes a record (sending to the ResultChan)
func processRecord(record map[string]interface{}) {
	defer ProcessingWG.Done()
	defer func() { <-workerSem }() // Release semaphore

	TransformMetrics.mu.Lock()
	TransformMetrics.Processed += 1
	TransformMetrics.mu.Unlock()

	// Create Record and apply transformations
	var rec models.Record
	if err := rec.Create(record); err != nil {
		recordWithError, _ := json.Marshal(record)
		log.WithFields(log.Fields{
			"record": json.RawMessage(recordWithError),
			"error":  err,
		}).Warn("error creating record - skipping...")

		TransformMetrics.mu.Lock()
		TransformMetrics.Skipped += 1
		TransformMetrics.mu.Unlock()
		return
	}

	if err := rec.Update(); err != nil {
		recordWithError, _ := json.Marshal(record)
		log.WithFields(log.Fields{
			"record": json.RawMessage(recordWithError), // logs as nested JSON, no escaping
			"error":  err,
		}).Warn("error processing record - skipping...")

		TransformMetrics.mu.Lock()
		TransformMetrics.Skipped += 1
		TransformMetrics.mu.Unlock()
		return
	}

	// Update bookmark for all successfully processed records (before filtering)
	// This ensures last_seen is updated even for unchanged records (deletion detection)
	models.State.UpdateBookmark(rec.ToMap())

	// Check if record passes bookmark filter
	if !rec.PassesBookmark() {
		TransformMetrics.mu.Lock()
		TransformMetrics.Filtered += 1
		TransformMetrics.mu.Unlock()
		return
	}

	ResultChan <- rec.ToMap()
}
