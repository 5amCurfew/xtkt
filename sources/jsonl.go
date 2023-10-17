package sources

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
)

func processJSONLRecord(record []byte, resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
	defer wg.Done()
	var data interface{}
	if err := json.Unmarshal(record, &data); err == nil {
		if processedData, err := lib.ProcessRecord(&data, state, config); err == nil && processedData != nil {
			resultChan <- processedData
		}
	}
}

func parseJSONL(file io.Reader, resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
	defer wg.Done()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		wg.Add(1)
		go processJSONLRecord(line, resultChan, config, state, wg)
	}
}

func collectResults(resultChan <-chan *interface{}) []interface{} {
	messages := []interface{}{}
	for msg := range resultChan {
		messages = append(messages, *msg)
	}
	return messages
}

func GenerateJSONLRecords(config lib.Config, state *lib.State) ([]interface{}, error) {
	var wg sync.WaitGroup
	resultChan := make(chan *interface{})

	file, err := os.Open(*config.URL)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var results []interface{}
	done := make(chan struct{})

	// Start a goroutine to collect records from the result channel
	go func() {
		results = collectResults(resultChan)
		close(done) // Signal completion
	}()

	wg.Add(1)
	go parseJSONL(file, resultChan, config, state, &wg)
	wg.Wait()
	close(resultChan)

	<-done
	return results, nil
}
