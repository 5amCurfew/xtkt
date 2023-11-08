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
func ParseJSONL(resultChan chan<- *interface{}, config lib.Config, state *lib.State, wg *sync.WaitGroup) {
	defer wg.Done()

	var records *bufio.Scanner

	if strings.HasPrefix(*config.URL, "http") {
		response, err := http.Get(*config.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseJSONL: http.Get failed")
		}
		defer response.Body.Close()
		records = bufio.NewScanner(response.Body)
	} else {
		file, err := os.Open(*config.URL)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseJSONL: os.Open failed")
		}
		defer file.Close()
		records = bufio.NewScanner(file)
	}

	for records.Scan() {
		line := records.Bytes()
		wg.Add(1)
		go lib.ParseRecord(line, resultChan, config, state, wg)
	}
}
