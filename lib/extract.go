package lib

import (
	"encoding/json"
	"runtime"
	"sync"

	"github.com/5amCurfew/xtkt/models"
	log "github.com/sirupsen/logrus"
)

var ExtractedChan = make(chan map[string]interface{}) // Unbuffered channel for extracted records; processing goroutines will read from this channel
var ResultChan = make(chan models.Record, 100)        // Buffered channel to prevent blocking on writes when processing is slower than extraction
var ProcessingWG sync.WaitGroup                       // WaitGroup to track processing goroutines
var workerSem = make(chan struct{}, runtime.NumCPU()) // Concurrency cap keeps CPU-bound transforms from outnumbering cores

// ExtractRecords begins streaming records from source (sending to ExtractedChan) and start goroutines to extract records (sending to ResultChan)
func ExtractRecords(sourceFunc func(*models.StreamConfig) error) {
	// begin a goroutine to stream records from source
	go func() {
		defer close(ExtractedChan)
		if err := sourceFunc(&models.Config); err != nil {
			log.WithFields(log.Fields{
				"error":       err,
				"source_type": models.Config.SourceType,
				"url":         models.Config.URL,
			}).Error("source extraction failed")
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
		}).Warn("record creation failed; not emitting")

		TransformMetrics.mu.Lock()
		TransformMetrics.TransformFailed += 1
		TransformMetrics.mu.Unlock()
		return
	}

	if err := rec.Update(); err != nil {
		recordWithError, _ := json.Marshal(record)
		log.WithFields(log.Fields{
			"record": json.RawMessage(recordWithError), // logs as nested JSON, no escaping
			"error":  err,
		}).Warn("record transformation failed; not emitting")

		TransformMetrics.mu.Lock()
		TransformMetrics.TransformFailed += 1
		TransformMetrics.mu.Unlock()
		return
	}

	// Evaluate bookmark filtering against the previous state before updating it
	// so new records still emit on the first run.
	passesBookmark := rec.PassesBookmark()

	// Check if record passes bookmark filter
	if !passesBookmark {
		// Unchanged records still refresh bookmark state so last_seen remains current.
		if !models.DISCOVER_MODE {
			models.State.QueueBookmarkUpdate(rec.ToMap(), false)
		}

		TransformMetrics.mu.Lock()
		TransformMetrics.FilteredBookmark += 1
		TransformMetrics.mu.Unlock()
		return
	}

	ResultChan <- rec
}
