package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"time"

	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// /////////////////////////////////////////////////////////
// PROCESS RECORDS
// /////////////////////////////////////////////////////////
func ProcessRecords(records *[]interface{}, state *State, config Config) error {
	if dropFieldsError := applyToRecords(DropFields, records, config); dropFieldsError != nil {
		return fmt.Errorf("error DROPPING FIELDS IN RECORD IN ProcessRecords: %v", dropFieldsError)
	}
	log.Info(fmt.Sprintf(`selected fields dropped from records at %s`, time.Now().UTC().Format(time.RFC3339)))

	if GenerateHashedFieldsError := applyToRecords(GenerateHashedFields, records, config); GenerateHashedFieldsError != nil {
		return fmt.Errorf("error GENERATING RECORD HASHED FIELD IN ProcessRecords: %v", GenerateHashedFieldsError)
	}
	log.Info(fmt.Sprintf(`selected fields hashed in records at %s`, time.Now().UTC().Format(time.RFC3339)))

	if GenerateSurrogateKeyFieldsError := applyToRecords(GenerateSurrogateKeyFields, records, config); GenerateSurrogateKeyFieldsError != nil {
		return fmt.Errorf("error GENERATING RECORD SURROGATE KEY IN ProcessRecords: %v", GenerateSurrogateKeyFieldsError)
	}
	log.Info(fmt.Sprintf(`surrogate keys added to records at %s`, time.Now().UTC().Format(time.RFC3339)))

	if reduceUsingBookmarkError := reduceUsingBookmark(records, state, config); reduceUsingBookmarkError != nil {
		return fmt.Errorf("error REDUCING RECORDS IN ProcessRecords: %v", reduceUsingBookmarkError)
	}
	log.Info(fmt.Sprintf(`%d records when reduced using bookmark at %s`, len(*records), time.Now().UTC().Format(time.RFC3339)))

	log.Info(fmt.Sprintf(`%d records processed at %s`, len(*records), time.Now().UTC().Format(time.RFC3339)))
	return nil
}

// /////////////////////////////////////////////////////////
// TRANSFORM RECORD
// /////////////////////////////////////////////////////////
func DropFields(record *interface{}, config Config) error {
	if config.Records.DropFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.DropFieldPaths {
				util.DropFieldAtPath(path, r)
			}
		} else {
			return fmt.Errorf("error parsing record in DropFields in record: %+v", r)
		}
	}
	return nil
}

func GenerateHashedFields(record *interface{}, config Config) error {
	if config.Records.SensitiveFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.SensitiveFieldPaths {
				if fieldValue := util.GetValueAtPath(path, r); fieldValue != nil {
					hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
					util.SetValueAtPath(path, r, hex.EncodeToString(hash[:]))
				} else {
					log.Warn(fmt.Sprintf("field path %s not found in record", path))
					continue
				}
			}
		} else {
			return fmt.Errorf("error parsing record in GenerateHashedFields in record: %+v", r)
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
		} else {
			log.Warn(fmt.Sprintf("unique_key field path %s not found in record", *config.Records.UniqueKeyPath))
		}
		r["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
		r["_sdc_time_extracted"] = time.Now().UTC().Format(time.RFC3339)
	} else {
		return fmt.Errorf("error parsing record in GenerateSurrogateKeyFields: %+v", r)
	}
	return nil
}

// /////////////////////////////////////////////////////////
// APPLY TO RECORDS
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
// REDUCE RECORDS
// /////////////////////////////////////////////////////////
func reduceUsingBookmark(records *[]interface{}, state *State, config Config) error {
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

			if config.Records.BookmarkPath != nil {
				switch path := *config.Records.BookmarkPath; {
				case reflect.DeepEqual(path, []string{"*"}):
					bookmarkCondition = !detectionSetContains(
						state.Value.Bookmarks[*config.StreamName].DetectionBookmark,
						r["_sdc_surrogate_key"].(string),
					)
				default:
					if BookmarkValue := util.GetValueAtPath(*config.Records.BookmarkPath, r); BookmarkValue != nil {
						bookmarkCondition = toString(BookmarkValue) > state.Value.Bookmarks[*config.StreamName].Bookmark
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
