package sources

import (
	"encoding/json"
	"fmt"
	"sync"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

var ResultChan = make(chan *interface{})
var ParsingWG sync.WaitGroup

func parse(record map[string]interface{}) {
	defer ParsingWG.Done()

	jsonData, _ := json.Marshal(record)
	var data interface{}
	if err := json.Unmarshal(jsonData, &data); err == nil {
		if processedData, err := lib.ProcessRecord(&data); err == nil && processedData != nil {
			ResultChan <- processedData
		} else if err != nil {
			log.Warn(fmt.Sprintf("error parsing record %s: %v", data, err))
		}
	}
}
