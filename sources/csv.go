package sources

import (
	"encoding/csv"
	"net/http"
	"os"
	"strings"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

func ParseCSV() {
	var data [][]string

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

	// Derive & Parse records
	header := data[0]
	for _, row := range data[1:] {
		record := make(map[string]interface{})
		for i, value := range row {
			record[header[i]] = value
		}

		ParsingWG.Add(1)
		go parse(record)
	}
}
