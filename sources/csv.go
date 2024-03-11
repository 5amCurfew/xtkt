package sources

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

// /////////////////////////////////////////////////////////
// PARSE
// /////////////////////////////////////////////////////////
func ParseCSV() {
	defer wg.Done()

	records, err := requestCSVRecords()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Info("parseCSV: requestCSVRecords failed")
		return
	}

	header := records[0]

	var parsingWG sync.WaitGroup

	// Introduce semaphore to limit concurrency
	sem := make(chan struct{}, maxConcurrency)

	for _, record := range records[1:] {
		parsingWG.Add(1)

		// "Acquire" a slot in the semaphore channel
		sem <- struct{}{}

		go func(record []string) {
			defer parsingWG.Done()

			// Ensure to release the slot after the goroutine finishes
			defer func() { <-sem }()

			data := make(map[string]interface{})
			for i, value := range record {
				data[header[i]] = value
			}

			jsonData, _ := json.Marshal(data)
			lib.ParseRecord(jsonData, resultChan)
		}(record)
	}
	parsingWG.Wait()
}

// /////////////////////////////////////////////////////////
// REQUEST
// /////////////////////////////////////////////////////////
func requestCSVRecords() ([][]string, error) {
	var records [][]string

	if strings.HasPrefix(*lib.ParsedConfig.URL, "http") {
		response, err := http.Get(*lib.ParsedConfig.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseCSV: http.Get failed")
		}
		defer response.Body.Close()
		reader := csv.NewReader(response.Body)
		records, _ = reader.ReadAll()
	} else {
		file, err := os.Open(*lib.ParsedConfig.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseCSV: os.Open failed")
		}
		defer file.Close()
		reader := csv.NewReader(file)
		records, _ = reader.ReadAll()
	}

	return records, nil
}
