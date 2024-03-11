package sources

import (
	"bufio"
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

	var parsingWG sync.WaitGroup

	// Introduce semaphore to limit concurrency
	sem := make(chan struct{}, maxConcurrency)

	for records.Scan() {
		line := records.Bytes()
		parsingWG.Add(1)

		// "Acquire" a slot in the semaphore channel
		sem <- struct{}{}

		go func() {
			defer parsingWG.Done()

			// Ensure to release the slot after the goroutine finishes
			defer func() { <-sem }()

			lib.ParseRecord(line, resultChan)
		}()
	}

	parsingWG.Wait()
}
