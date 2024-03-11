package sources

import (
	"bufio"
	"net/http"
	"os"
	"strings"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

// /////////////////////////////////////////////////////////
// PARSE
// /////////////////////////////////////////////////////////
func ParseJSONL() {
	defer wg.Done()

	var records *bufio.Scanner

	if strings.HasPrefix(*lib.ParsedConfig.URL, "http") {
		response, err := http.Get(*lib.ParsedConfig.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseJSONL: http.Get failed")
		}
		defer response.Body.Close()
		records = bufio.NewScanner(response.Body)
	} else {
		file, err := os.Open(*lib.ParsedConfig.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseJSONL: os.Open failed")
		}
		defer file.Close()
		records = bufio.NewScanner(file)
	}

	sem := make(chan struct{}, *lib.ParsedConfig.MaxConcurrency)
	for records.Scan() {
		data := records.Bytes()
		// Make a copy of data to avoid data races
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		// "Acquire" a slot in the semaphore channel
		sem <- struct{}{}
		parsingWG.Add(1)

		go func(dataCopy []byte) {
			defer parsingWG.Done()

			// Ensure to release the slot after the goroutine finishes
			defer func() { <-sem }()

			//jsonData, _ := json.Marshal(dataCopy)
			lib.ParseRecord(dataCopy, resultChan)
		}(dataCopy)
	}

	parsingWG.Wait()
}
