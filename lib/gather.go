package lib

import "sync"

// /////////////////////////////////////////////////////////
// COLLECT RECORDS
// Append messages from resultChan to slice
// /////////////////////////////////////////////////////////
func CollectResults(resultChan <-chan *interface{}) []interface{} {
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
func GatherRecords(f func(resultChan chan<- *interface{}, config Config, state *State, wg *sync.WaitGroup), config Config, state *State) ([]interface{}, error) {
	var wg sync.WaitGroup
	resultChan := make(chan *interface{})

	var results []interface{}
	completeSignal := make(chan struct{})

	// ///////////////////////////////////////////////////////
	// Start a goroutine CollectResults()
	// CollectResults() recieves messages from resultChan and appends messages to a slice
	// Defer closing the channel until completion of all records (completeSignal)
	// ///////////////////////////////////////////////////////
	go func() {
		results = CollectResults(resultChan)
		close(completeSignal)
	}()

	// ///////////////////////////////////////////////////////
	// Start a goroutine applying the Parse<SOURCE>() function (f)
	// Parse<SOURCE>() extracts records and starts a goroutine for each record applying the ParseRecord()
	// ParseRecord() applies the processRecord function to each record
	// processRecord() applies transformations to a record and sends the records to the resultChan
	// See sources/<SOURCE>:Parse<SOURCE>
	// See lib/record.go:ParseRecord()
	// see lib/record.go:processRecord()
	// ///////////////////////////////////////////////////////
	wg.Add(1)
	go f(resultChan, config, state, &wg)
	wg.Wait()
	close(resultChan)

	<-completeSignal
	return results, nil
}
