package sources

import (
	"bufio"
	"net/http"
	"os"
	"strings"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
)

// /////////////////////////////////////////////////////////
// PARSE
// /////////////////////////////////////////////////////////
func ParseJSONL(resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
	defer wg.Done()

	var records *bufio.Scanner

	if strings.HasPrefix(*config.URL, "http") {
		response, _ := http.Get(*config.URL)
		defer response.Body.Close()
		records = bufio.NewScanner(response.Body)
	} else {
		file, _ := os.Open(*config.URL)
		defer file.Close()
		records = bufio.NewScanner(file)
	}

	for records.Scan() {
		line := records.Bytes()
		wg.Add(1)
		go lib.ParseRecord(line, resultChan, config, state, wg)
	}
}
