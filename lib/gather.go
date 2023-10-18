package lib

import "sync"

// /////////////////////////////////////////////////////////
// GATHER
// /////////////////////////////////////////////////////////
func GatherRecords(f func(resultChan chan<- *interface{}, config Config, state *State, wg *sync.WaitGroup), config Config, state *State) ([]interface{}, error) {
	var wg sync.WaitGroup
	resultChan := make(chan *interface{})

	var results []interface{}
	done := make(chan struct{})

	// Start a goroutine to collect records from the result channel
	go func() {
		results = CollectResults(resultChan)
		close(done) // Signal completion
	}()

	wg.Add(1)
	go f(resultChan, config, state, &wg)
	wg.Wait()
	close(resultChan)

	<-done
	return results, nil
}
