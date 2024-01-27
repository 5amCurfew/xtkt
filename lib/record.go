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
// processRecord() and send to resultChan
// /////////////////////////////////////////////////////////
func ParseRecord(record []byte, resultChan chan<- *interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var data interface{}
	if err := json.Unmarshal(record, &data); err == nil {
		if processedData, err := processRecord(&data); err == nil && processedData != nil {
			resultChan <- processedData
		}
	}
}

// /////////////////////////////////////////////////////////
// PROCESS RECORD
// Drop fields, generate hashed fields, generate surrogate keys & apply bookmark on a record
// /////////////////////////////////////////////////////////
func processRecord(record *interface{}) (*interface{}, error) {
	if dropFieldsError := dropFields(record); dropFieldsError != nil {
		return nil, fmt.Errorf("error dropping fields in ProcessRecord: %v", dropFieldsError)
	}

	if generateHashedFieldsError := generateHashedFields(record); generateHashedFieldsError != nil {
		return nil, fmt.Errorf("error generating hashed field in ProcessRecord: %v", generateHashedFieldsError)
	}

	if generateSurrogateKeyFieldsError := generateSurrogateKeyFields(record); generateSurrogateKeyFieldsError != nil {
		return nil, fmt.Errorf("error generating surrogate keys in ProcessRecords: %v", generateSurrogateKeyFieldsError)
	}

	if keep, recordVersusBookmarkError := recordVersusBookmark(record); recordVersusBookmarkError != nil {
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
func dropFields(record *interface{}) error {
	if ParsedConfig.Records.DropFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *ParsedConfig.Records.DropFieldPaths {
				util.DropFieldAtPath(path, r)
			}
		} else {
			return fmt.Errorf("error parsing record in DropFields in record: %+v", r)
		}
	}
	return nil
}

func generateHashedFields(record *interface{}) error {
	if ParsedConfig.Records.SensitiveFieldPaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *ParsedConfig.Records.SensitiveFieldPaths {
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

func generateSurrogateKeyFields(record *interface{}) error {
	if r, parsed := (*record).(map[string]interface{}); parsed {
		h := sha256.New()
		h.Write([]byte(toString(r)))
		if util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, r) != nil {
			r["_sdc_natural_key"] = util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, r)
		} else {
			log.Warn(fmt.Sprintf("unique_key field path %s not found in record", *ParsedConfig.Records.UniqueKeyPath))
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
func recordVersusBookmark(record *interface{}) (bool, error) {
	bookmarkCondition := false
	if r, parsed := (*record).(map[string]interface{}); parsed {
		if ParsedConfig.Records.BookmarkPath != nil {
			switch path := *ParsedConfig.Records.BookmarkPath; {
			case reflect.DeepEqual(path, []string{"*"}):
				bookmarkCondition = !detectionSetContains(
					ParsedState.Value.Bookmarks[*ParsedConfig.StreamName].DetectionBookmark,
					r["_sdc_surrogate_key"].(string),
				)
			default:
				if BookmarkValue := util.GetValueAtPath(*ParsedConfig.Records.BookmarkPath, r); BookmarkValue != nil {
					bookmarkCondition = toString(BookmarkValue) > ParsedState.Value.Bookmarks[*ParsedConfig.StreamName].Bookmark
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
