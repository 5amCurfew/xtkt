package sources

import (
	"bufio"
	"os"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
)

func parseJSONL(resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
	defer wg.Done()
	file, _ := os.Open(*config.URL)
	defer file.Close()

	records := bufio.NewScanner(file)

	for records.Scan() {
		line := records.Bytes()
		wg.Add(1)
		go lib.ParseRecord(line, resultChan, config, state, wg)
	}
}

func GenerateJSONLRecords(config lib.Config, state *lib.State) ([]interface{}, error) {
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
	go parseJSONL(resultChan, config, state, &wg)
	wg.Wait()
	close(resultChan)

	<-done
	return results, nil
}
