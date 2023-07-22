package sources

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
)

// curl -X POST -H "Content-Type: application/json" -d '{"key1":"value1","key2":"value2"}' http://localhost:8080/messages

func StartListening(config lib.Config) {
	recordStore := &RecordStore{}

	http.HandleFunc("/messages", handleIncomingRecords(recordStore, config))
	go func() {
		if err := http.ListenAndServe(":"+*config.Listen.Port, nil); err != nil {
			fmt.Println("Server error:", err)
			os.Exit(1)
		}
	}()
	recordStore.StartTimer(config)
	log.Info(fmt.Sprintf(`xtkt started listening on port %s at %s`, *config.Listen.Port, time.Now().UTC().Format(time.RFC3339)))

	// Keep the main goroutine running
	select {}
}

type RecordStore struct {
	sync.Mutex
	records []*interface{}
	timer   *time.Timer
}

func (rs *RecordStore) AddRecord(record interface{}) {
	rs.Lock()
	defer rs.Unlock()
	rs.records = append(rs.records, &record)
}

func (rs *RecordStore) ClearRecords() {
	rs.Lock()
	defer rs.Unlock()
	rs.records = []*interface{}{}
}

func (rs *RecordStore) StartTimer(config lib.Config) {
	rs.Lock()
	defer rs.Unlock()

	if rs.timer == nil {
		rs.timer = time.NewTimer(time.Duration(*config.Listen.CollectionInterval) * time.Second)
		go func() {
			<-rs.timer.C
			rs.processRecords(config)
			rs.ClearRecords()
			log.Info(fmt.Sprintf(`record cache cleared at %s`, time.Now().UTC().Format(time.RFC3339)))

			rs.Lock()
			rs.timer.Stop() // Stop the timer before starting it again
			rs.timer = nil  // Reset the timer to nil
			rs.Unlock()

			rs.StartTimer(config) // Start the timer again
		}()
	} else {
		rs.timer.Stop() // Stop the timer before resetting it
		rs.timer.Reset(time.Duration(*config.Listen.CollectionInterval) * time.Second)
	}
}

func (rs *RecordStore) processRecords(config lib.Config) {
	rs.Lock()
	log.Info(fmt.Sprintf(`records stored at %s: %d`, time.Now().UTC().Format(time.RFC3339), len(rs.records)))
	log.Info(fmt.Sprintf(`records processing started at %s`, time.Now().UTC().Format(time.RFC3339)))

	defer rs.Unlock()

	if len(rs.records) > 0 {
		for i := range rs.records {
			record := rs.records[i]
			lib.GenerateHashedRecordsFields(record, config)
			lib.GenerateSurrogateKey(record, config)
			rs.records[i] = record
		}

		// Create a new slice to store the records
		records := make([]interface{}, len(rs.records))
		// Iterate over the *interface{} pointers and copy the values to the new slice
		for i, recordPtr := range rs.records {
			records[i] = *recordPtr
		}
		if processSchemaError := lib.ProcessSchema(records, config); processSchemaError != nil {
			log.Error("error PROCESSING SCHEMA: %w", processSchemaError)
		}

		for _, record := range rs.records {
			r := (*record).(map[string]interface{})
			message := lib.Message{
				Type:   "RECORD",
				Record: r,
				Stream: *config.StreamName,
			}

			messageJson, err := json.Marshal(message)
			if err != nil {
				log.Error("Error marshaling message:", err)
				continue
			}

			fmt.Println(string(messageJson))
		}
	}
}

func handleIncomingRecords(recordStore *RecordStore, config lib.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Content-Type") != "application/json" {
			log.Println("only Content-Type: application/json is supported")
			return
		}

		var record interface{}
		decoder := json.NewDecoder(req.Body)
		if err := decoder.Decode(&record); err != nil {
			// error parsing the JSON, return the original output
			log.Info(fmt.Sprintf(`error JSON.UNMARSHAL REQUEST at %s, skipping`, time.Now().UTC().Format(time.RFC3339)))
			return
		}

		recordStore.AddRecord(record)
		log.Info(fmt.Sprintf(`record added to recordStore at %s`, time.Now().UTC().Format(time.RFC3339)))
	}
}
