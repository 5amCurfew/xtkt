package sources

import (
	"bufio"
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
func ParseJSONL() {
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

	// Derive & Parse records
	for records.Scan() {
		data := records.Bytes()
		// Make a copy of data to avoid data races
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		record := make(map[string]interface{})
		json.Unmarshal(dataCopy, &record)

		ParsingWG.Add(1)
		go parse(record)
	}
}
