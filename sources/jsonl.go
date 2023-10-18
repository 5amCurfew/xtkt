package sources

import (
	"bufio"
	"os"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
)

// /////////////////////////////////////////////////////////
// PARSE
// /////////////////////////////////////////////////////////
func ParseJSONL(resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
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
