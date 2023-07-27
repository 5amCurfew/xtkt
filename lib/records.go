package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

// /////////////////////////////////////////////////////////
// TRANSFORM FIELD(s)
// /////////////////////////////////////////////////////////
func GenerateHashedFields(record *interface{}, config Config) error {
	if config.Records.SensitiveFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.SensitiveFieldPaths {
				if fieldValue := util.GetValueAtPath(path, r); fieldValue != nil {
					hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
					util.SetValueAtPath(path, r, hex.EncodeToString(hash[:]))
				} else {
					continue
				}
			}
		} else {
			return fmt.Errorf("error PARSING RECORD in GenerateHashedFields in record: %+v", r)
		}
	}
	return nil
}

func GenerateSurrogateKeyFields(record *interface{}, config Config) error {
	if r, parsed := (*record).(map[string]interface{}); parsed {
		h := sha256.New()
		h.Write([]byte(toString(r)))
		if util.GetValueAtPath(*config.Records.UniqueKeyPath, r) != nil {
			r["_sdc_natural_key"] = util.GetValueAtPath(*config.Records.UniqueKeyPath, r)
		}
		r["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
		r["_sdc_time_extracted"] = time.Now().UTC().Format(time.RFC3339)
	} else {
		return fmt.Errorf("error PARSING RECORD in GenerateSurrogateKeyFields in record: %+v", r)
	}
	return nil
}

// /////////////////////////////////////////////////////////
// DROP FIELD(s)
// /////////////////////////////////////////////////////////
func DropFields(record *interface{}, config Config) error {
	if config.Records.DropFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.DropFieldPaths {
				util.DropFieldAtPath(path, r)
			}
		} else {
			return fmt.Errorf("error PARSING RECORD in DropFields in record: %+v", r)
		}
	}
	return nil
}

// /////////////////////////////////////////////////////////
// UPDATE RECORDS PER FIELD FUNCTION
// /////////////////////////////////////////////////////////
func applyToRecords(f func(*interface{}, Config) error, records *[]interface{}, config Config) error {
	recordChan := make(chan int, len(*records))
	resultChan := make(chan error, len(*records))
	var wg sync.WaitGroup

	// Launch goroutines to process the records
	for i := 0; i < len(*records); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			index := <-recordChan
			record := (*records)[index]
			err := f(&record, config)
			if err != nil {
				resultChan <- fmt.Errorf("error applying function to record %d: %s", index, err.Error())
			}
			(*records)[index] = record
			resultChan <- nil
		}(i)
		recordChan <- i
	}

	wg.Wait()
	close(recordChan)
	close(resultChan)

	for err := range resultChan {
		if err != nil {
			return fmt.Errorf("error APPLYING TO RECORDS: %v", err)
		}
	}
	return nil
}

// /////////////////////////////////////////////////////////
// FILTER RECORDS
// /////////////////////////////////////////////////////////
func filterRecords(records *[]interface{}, config Config) error {
	if config.Records.FilterFieldPath != nil {
		var (
			filteredRecords []interface{}
			wg              sync.WaitGroup
			mu              sync.Mutex // Mutex to synchronize access to reducedRecords
		)

		// Launch goroutines to process the records
		for _, record := range *records {
			// Increment the wait group counter
			wg.Add(1)
			go func(record interface{}) {
				defer wg.Done()

				r := record.(map[string]interface{})
				if !FilterBreached(r, config) {
					mu.Lock()
					filteredRecords = append(filteredRecords, record)
					mu.Unlock()
				}
			}(record)
		}

		wg.Wait()
		*records = filteredRecords
		return nil
	}
	return nil
}

// Also used in listen.go
func FilterBreached(record map[string]interface{}, config Config) bool {
	if config.Records.FilterFieldPath == nil {
		return false
	} else {
		for _, filter := range *config.Records.FilterFieldPath {
			if value := util.GetValueAtPath(filter.FieldPath, record); value != nil {
				switch filter.Operation {
				case "less_than":
					if value.(string) >= filter.Value {
						return true
					}
				case "greater_than":
					if value.(string) <= filter.Value {
						return true
					}
				case "equal_to":
					if value != filter.Value {
						return true
					}
				case "not_equal_to":
					if value == filter.Value {
						return true
					}
				default:
					return false
				}
			} else {
				return false
			}
		}
		return false
	}
}

// /////////////////////////////////////////////////////////
// REDUCE RECORDS
// /////////////////////////////////////////////////////////
func reduceRecords(records *[]interface{}, state *State, config Config) error {
	var (
		reducedRecords []interface{}
		wg             sync.WaitGroup
		mu             sync.Mutex // Mutex to synchronize access to reducedRecords
	)

	// Iterate over the records slice
	for _, record := range *records {
		// Increment the wait group counter
		wg.Add(1)

		go func(record interface{}) {
			defer wg.Done()

			r := record.(map[string]interface{})
			bookmarkCondition := false

			if config.Records.PrimaryBookmarkPath != nil {
				switch path := *config.Records.PrimaryBookmarkPath; {
				case reflect.DeepEqual(path, []string{"*"}):
					bookmarkCondition = !detectionSetContains(
						state.Value.Bookmarks[*config.StreamName].DetectionBookmark,
						r["_sdc_surrogate_key"].(string),
					)
				default:
					if primaryBookmarkValue := util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r); primaryBookmarkValue != nil {
						bookmarkCondition = toString(primaryBookmarkValue) > state.Value.Bookmarks[*config.StreamName].PrimaryBookmark
					} else {
						bookmarkCondition = true
					}
				}

			} else {
				bookmarkCondition = true
			}

			if bookmarkCondition {
				mu.Lock()
				reducedRecords = append(reducedRecords, r)
				mu.Unlock()
			}
		}(record)
	}

	wg.Wait()
	*records = reducedRecords
	return nil
}

// /////////////////////////////////////////////////////////
// PROCESS RECORDS
// /////////////////////////////////////////////////////////
func ProcessRecords(records *[]interface{}, state *State, config Config) error {
	if filterRecordsError := filterRecords(records, config); filterRecordsError != nil {
		return fmt.Errorf("error DROPPING FIELDS IN RECORD IN ProcessRecords: %v", filterRecordsError)
	}

	if dropFieldsError := applyToRecords(DropFields, records, config); dropFieldsError != nil {
		return fmt.Errorf("error DROPPING FIELDS IN RECORD IN ProcessRecords: %v", dropFieldsError)
	}

	if GenerateHashedFieldsError := applyToRecords(GenerateHashedFields, records, config); GenerateHashedFieldsError != nil {
		return fmt.Errorf("error GENERATING RECORD HASHED FIELD IN ProcessRecords: %v", GenerateHashedFieldsError)
	}

	if GenerateSurrogateKeyFieldsError := applyToRecords(GenerateSurrogateKeyFields, records, config); GenerateSurrogateKeyFieldsError != nil {
		return fmt.Errorf("error GENERATING RECORD SURROGATE KEY IN ProcessRecords: %v", GenerateSurrogateKeyFieldsError)
	}

	if reduceRecordsError := reduceRecords(records, state, config); reduceRecordsError != nil {
		return fmt.Errorf("error REDUCING RECORDS IN ProcessRecords: %v", reduceRecordsError)
	}

	return nil
}
