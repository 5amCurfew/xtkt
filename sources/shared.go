package sources

import (
	"encoding/json"
	"fmt"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

var parseRecordChan = make(chan map[string]interface{})
var ResultChan = make(chan *interface{})
var ParsingWG sync.WaitGroup

// ParseRecords: begin streaming records from source and subsequently listen to them on parseRecordChan for processing
func ParseRecords(streamFunc func(lib.Config) error) {
	go func() {
		defer close(parseRecordChan)
		if err := streamFunc(lib.ParsedConfig); err != nil {
			log.WithFields(log.Fields{"error": err}).Info("ParseRecords: stream function failed")
		}
	}()

	// begin a goroutine for each record processing the record and sending to the ResultChan
	for record := range parseRecordChan {
		ParsingWG.Add(1)
		go process(record)
	}
}

// process a record and send to ResultChan
func process(record map[string]interface{}) {
	defer ParsingWG.Done()

	jsonData, _ := json.Marshal(record)
	var data interface{}
	if err := json.Unmarshal(jsonData, &data); err == nil {
		if processedData, err := lib.ProcessRecord(&data); err == nil && processedData != nil {
			ResultChan <- processedData
		} else if err != nil {
			log.Warn(fmt.Sprintf("error parsing record %s: %v", data, err))
		}
	}
}
