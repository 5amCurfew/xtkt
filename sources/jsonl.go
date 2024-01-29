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
	for records.Scan() {
		line := records.Bytes()
		parsingWG.Add(1)
		go func() {
			defer parsingWG.Done()
			lib.ParseRecord(line, resultChan)
		}()
	}

	parsingWG.Wait()
}
