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
func ParseCSV(resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
	defer wg.Done()

	var records [][]string

	if strings.HasPrefix(*config.URL, "http") {
		response, err := http.Get(*config.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseCSV: http.Get failed")
		}
		defer response.Body.Close()
		reader := csv.NewReader(response.Body)
		records, _ = reader.ReadAll()
	} else {
		file, err := os.Open(*config.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseCSV: os.Open failed")
		}
		defer file.Close()
		reader := csv.NewReader(file)
		records, _ = reader.ReadAll()
	}

	header := records[0]

	var transformWG sync.WaitGroup

	for _, record := range records[1:] {
		transformWG.Add(1)
		go func(record []string) {
			defer transformWG.Done()

			data := make(map[string]interface{})
			for i, value := range record {
				data[header[i]] = value
			}

			jsonData, _ := json.Marshal(data)

			wg.Add(1)
			go lib.ParseRecord(jsonData, resultChan, config, state, wg)
		}(record)
	}

	transformWG.Wait()
}
