package sources

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"os"
	"strings"

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

	sem := make(chan struct{}, *lib.ParsedConfig.MaxConcurrency)
	for _, record := range records[1:] {
		// "Acquire" a slot in the semaphore channel
		sem <- struct{}{}
		parsingWG.Add(1)

		go func(record interface{}) {
			defer parsingWG.Done()

			// Ensure to release the slot after the goroutine finishes
			defer func() { <-sem }()

			jsonData, _ := json.Marshal(record)
			lib.ParseRecord(jsonData, resultChan)
		}(record)
	}
	parsingWG.Wait()
}

// /////////////////////////////////////////////////////////
// REQUEST
// /////////////////////////////////////////////////////////
func requestCSVRecords() ([]map[string]interface{}, error) {
	var data [][]string
	var records []map[string]interface{}

	if strings.HasPrefix(*lib.ParsedConfig.URL, "http") {
		response, err := http.Get(*lib.ParsedConfig.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseCSV: http.Get failed")
		}
		defer response.Body.Close()
		reader := csv.NewReader(response.Body)
		data, _ = reader.ReadAll()
	} else {
		file, err := os.Open(*lib.ParsedConfig.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseCSV: os.Open failed")
		}
		defer file.Close()
		reader := csv.NewReader(file)
		data, _ = reader.ReadAll()
	}

	header := data[0]
	for _, row := range data[1:] {
		record := make(map[string]interface{})
		for i, value := range row {
			record[header[i]] = value
		}
		records = append(records, record)
	}

	return records, nil
}
