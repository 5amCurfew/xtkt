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

func GenerateHashedRecordsFields(record *interface{}, config Config) error {
	if config.Records.SensitiveFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.SensitiveFieldPaths {
				if fieldValue := util.GetValueAtPath(path, r); fieldValue != nil {
					hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
					util.SetValueAtPath(path, r, hex.EncodeToString(hash[:]))
				} else {
					return fmt.Errorf("error PARSING RECORD FIELD in generateHashedRecordsFields in record: %+v", r)
				}
			}
		} else {
			return fmt.Errorf("error PARSING RECORD in generateHashedRecordsFields in record: %+v", r)
		}
	}
	return nil
}

func GenerateSurrogateKey(record *interface{}, config Config) error {
	if r, parsed := (*record).(map[string]interface{}); parsed {
		h := sha256.New()
		h.Write([]byte(toString(r)))
		r["_sdc_natural_key"] = util.GetValueAtPath(*config.Records.UniqueKeyPath, r)
		r["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
		r["_sdc_time_extracted"] = time.Now().UTC().Format(time.RFC3339)
	} else {
		return fmt.Errorf("error PARSING RECORD in generateSurrogateKey in record: %+v", r)
	}
	return nil
}

func DropFields(record *interface{}, config Config) error {
	if config.Records.DropFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.DropFieldPaths {
				dropFieldAtPath(path, r)
			}
		} else {
			return fmt.Errorf("error PARSING RECORD in DropFields in record: %+v", r)
		}
	}
	return nil
}

func dropFieldAtPath(path []string, record map[string]interface{}) error {
	if len(path) == 0 {
		return nil
	}

	var currentMap = record
	for i := 0; i < len(path)-1; i++ {
		key := path[i]
		value, exists := currentMap[key]
		if !exists {
			return nil
		}

		if nestedMap, ok := value.(map[string]interface{}); ok {
			currentMap = nestedMap
		} else {
			return nil
		}
	}

	lastKey := path[len(path)-1]
	// Delete the field from the nested map if it exists
	if _, exists := currentMap[lastKey]; exists {
		delete(currentMap, lastKey)
		return nil
	}

	return nil
}

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
					primaryBookmarkValue := util.GetValueAtPath(*config.Records.PrimaryBookmarkPath, r)
					bookmarkCondition = toString(primaryBookmarkValue) > state.Value.Bookmarks[*config.StreamName].PrimaryBookmark
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
	// Update the original records slice with the reduced records
	*records = reducedRecords
	return nil
}

func ProcessRecords(records *[]interface{}, state *State, config Config) error {
	dropFieldsError := applyToRecords(DropFields, records, config)
	if dropFieldsError != nil {
		return fmt.Errorf("error DROPPING FIELDS IN RECORD IN ProcessRecords: %v", dropFieldsError)
	}

	generateHashedRecordsFieldsError := applyToRecords(GenerateHashedRecordsFields, records, config)
	if generateHashedRecordsFieldsError != nil {
		return fmt.Errorf("error GENERATING RECORD HASHED FIELD IN ProcessRecords: %v", generateHashedRecordsFieldsError)
	}

	generateSurrogateKeyError := applyToRecords(GenerateSurrogateKey, records, config)
	if generateSurrogateKeyError != nil {
		return fmt.Errorf("error GENERATING RECORD SURROGATE KEY IN ProcessRecords: %v", generateSurrogateKeyError)
	}

	reduceRecordsError := reduceRecords(records, state, config)
	if reduceRecordsError != nil {
		return fmt.Errorf("error REDUCING RECORDS IN ProcessRecords: %v", reduceRecordsError)
	}

	return nil
}
