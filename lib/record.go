package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// /////////////////////////////////////////////////////////
// PARSE RECORD
// /////////////////////////////////////////////////////////
func ParseRecord(record []byte, resultChan chan<- *interface{}, config Config, state *State, wg *sync.WaitGroup) {
	defer wg.Done()
	var data interface{}
	if err := json.Unmarshal(record, &data); err == nil {
		if processedData, err := processRecord(&data, state, config); err == nil && processedData != nil {
			resultChan <- processedData
		}
	}
}

func CollectResults(resultChan <-chan *interface{}) []interface{} {
	messages := []interface{}{}
	for msg := range resultChan {
		messages = append(messages, *msg)
	}
	return messages
}

// /////////////////////////////////////////////////////////
// PROCESS RECORD
// /////////////////////////////////////////////////////////
func processRecord(record *interface{}, state *State, config Config) (*interface{}, error) {
	if dropFieldsError := dropFields(record, config); dropFieldsError != nil {
		return nil, fmt.Errorf("error dropping fields in ProcessRecord: %v", dropFieldsError)
	}

	if generateHashedFieldsError := generateHashedFields(record, config); generateHashedFieldsError != nil {
		return nil, fmt.Errorf("error generating hashed field in ProcessRecord: %v", generateHashedFieldsError)
	}

	if generateSurrogateKeyFieldsError := generateSurrogateKeyFields(record, config); generateSurrogateKeyFieldsError != nil {
		return nil, fmt.Errorf("error generating surrogate keys in ProcessRecords: %v", generateSurrogateKeyFieldsError)
	}

	if keep, recordVersusBookmarkError := recordVersusBookmark(record, state, config); recordVersusBookmarkError != nil {
		return nil, fmt.Errorf("error using bookmark in ProcessRecords: %v", recordVersusBookmarkError)
	} else {
		if keep {
			return record, nil
		}
	}
	return nil, nil
}

// /////////////////////////////////////////////////////////
// TRANSFORM RECORD
// /////////////////////////////////////////////////////////
func dropFields(record *interface{}, config Config) error {
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

func generateHashedFields(record *interface{}, config Config) error {
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
			return fmt.Errorf("error parsing record in generateHashedFields in record: %+v", r)
		}
	}
	return nil
}

func generateSurrogateKeyFields(record *interface{}, config Config) error {
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
		return fmt.Errorf("error parsing record in generateSurrogateKeyFields: %+v", r)
	}
	return nil
}

// /////////////////////////////////////////////////////////
// APPLY BOOKMARK TO RECORD
// /////////////////////////////////////////////////////////
func recordVersusBookmark(record *interface{}, state *State, config Config) (bool, error) {
	bookmarkCondition := false
	if r, parsed := (*record).(map[string]interface{}); parsed {
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
	} else {
		return false, fmt.Errorf("error parsing record in recordVersusBookmark: %+v", r)
	}
	return bookmarkCondition, nil
}
