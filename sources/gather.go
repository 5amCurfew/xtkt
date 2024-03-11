package sources

import (
	"sync"
)

type sourceFunction = func()

var resultChan = make(chan *interface{})
var wg sync.WaitGroup
var parsingWG sync.WaitGroup

// /////////////////////////////////////////////////////////
// COLLECT RECORDS
// Append messages from resultChan to slice until resultChan is closed
// /////////////////////////////////////////////////////////
func CollectResults() []interface{} {
	messages := []interface{}{}
	for msg := range resultChan {
		messages = append(messages, *msg)
	}
	return messages
}

// /////////////////////////////////////////////////////////
// GATHER
// Gather processed records
// /////////////////////////////////////////////////////////
func GatherRecords(f sourceFunction) ([]interface{}, error) {
	var results []interface{}
	completeSignal := make(chan struct{})

	// ///////////////////////////////////////////////////////
	// Start a goroutine CollectResults()
	// CollectResults() recieves messages from resultChan and appends messages to a slice
	// Defer closing the channel until completion of all records (completeSignal)
	// ///////////////////////////////////////////////////////
	go func() {
		results = CollectResults()
		close(completeSignal)
	}()

	// ///////////////////////////////////////////////////////
	// Start a goroutine applying the Parse<SOURCE>() function (f)
	// Parse<SOURCE>() extracts records and starts a goroutine for each record applying the ParseRecord()
	// ParseRecord() applies the processRecord function to each record and on success sends to the resultChan
	// processRecord() applies transformations to a record and returns the transformed record
	// See sources/<SOURCE>:Parse<SOURCE>
	// See lib/record.go:ParseRecord()
	// see lib/record.go:processRecord()
	// ///////////////////////////////////////////////////////
	wg.Add(1)
	go f()
	wg.Wait()
	close(resultChan)

	<-completeSignal
	return results, nil
}
