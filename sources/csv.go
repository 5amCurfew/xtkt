package sources

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
)

func parseCSV(resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
	defer wg.Done()
	file, _ := os.Open(*config.URL)
	defer file.Close()

	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()
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

func GenerateCSVRecords(config lib.Config, state *lib.State) ([]interface{}, error) {
	var wg sync.WaitGroup
	resultChan := make(chan *interface{})

	var results []interface{}
	done := make(chan struct{})

	// Start a goroutine to collect records from the result channel
	go func() {
		results = lib.CollectResults(resultChan)
		close(done) // Signal completion
	}()

	wg.Add(1)
	go parseCSV(resultChan, config, state, &wg)
	wg.Wait()
	close(resultChan)

	<-done
	return results, nil
}
