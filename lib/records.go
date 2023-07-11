package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"time"
)

func getValueAtPath(path []string, input map[string]interface{}) interface{} {
	if len(path) > 0 {
		if check, ok := input[path[0]]; !ok || check == nil {
			return nil
		}
		if len(path) == 1 {
			return input[path[0]]
		}

		key := path[0]
		path = path[1:]

		nextInput, _ := input[key].(map[string]interface{})

		return getValueAtPath(path, nextInput)
	} else {
		return input
	}
}

func setValueAtPath(path []string, input map[string]interface{}, value interface{}) {
	if len(path) == 1 {
		input[path[0]] = value
		return
	}

	key := path[0]
	path = path[1:]

	if _, ok := input[key]; !ok {
		input[key] = make(map[string]interface{})
	}

	setValueAtPath(path, input[key].(map[string]interface{}), value)
}

func generateHashedRecordsFields(record *interface{}, config Config) error {
	if config.Records.SensitivePaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.SensitivePaths {
				if fieldValue := getValueAtPath(path, r); fieldValue != nil {
					hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
					setValueAtPath(path, r, hex.EncodeToString(hash[:]))
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

func generateSurrogateKey(record *interface{}, config Config) error {
	if r, parsed := (*record).(map[string]interface{}); parsed {
		h := sha256.New()
		h.Write([]byte(toString(r)))
		r["_sdc_natural_key"] = getValueAtPath(*config.Records.UniqueKeyPath, r)
		r["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
		r["_sdc_time_extracted"] = time.Now().UTC().Format(time.RFC3339)
	} else {
		return fmt.Errorf("error PARSING RECORD in generateSurrogateKey in record: %+v", r)
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
	var reducedRecords []interface{}

	for _, record := range *records {
		r, parsed := record.(map[string]interface{})
		if !parsed {
			return fmt.Errorf("error PARSING RECORD IN reduceRecords: %v", record)
		}

		bookmarkCondition := false

		if config.Records.PrimaryBookmarkPath != nil {
			switch path := *config.Records.PrimaryBookmarkPath; {
			case reflect.DeepEqual(path, []string{"*"}):
				bookmarkCondition = !detectionSetContains(
					state.Value.Bookmarks[*config.StreamName].DetectionBookmark,
					r["_sdc_surrogate_key"].(string),
				)
			default:
				primaryBookmarkValue := getValueAtPath(*config.Records.PrimaryBookmarkPath, r)
				bookmarkCondition = toString(primaryBookmarkValue) > state.Value.Bookmarks[*config.StreamName].PrimaryBookmark
			}

		} else {
			bookmarkCondition = true
		}

		if bookmarkCondition {
			reducedRecords = append(reducedRecords, r)
		}
	}

	*records = reducedRecords
	return nil
}

func ProcessRecords(records *[]interface{}, state *State, config Config) error {
	generateHashedRecordsFieldsError := applyToRecords(generateHashedRecordsFields, records, config)
	if generateHashedRecordsFieldsError != nil {
		return fmt.Errorf("error GENERATING RECORD HASHED FIELD IN ProcessRecords: %v", generateHashedRecordsFieldsError)
	}

	generateSurrogateKeyError := applyToRecords(generateSurrogateKey, records, config)
	if generateSurrogateKeyError != nil {
		return fmt.Errorf("error GENERATING RECORD SURROGATE KEY IN ProcessRecords: %v", generateSurrogateKeyError)
	}

	reduceRecordsError := reduceRecords(records, state, config)
	if reduceRecordsError != nil {
		return fmt.Errorf("error REDUCING RECORDS IN ProcessRecords: %v", reduceRecordsError)
	}

	return nil
}
