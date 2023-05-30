package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

func generateHashedRecordsFields(record *interface{}, config Config) {
	if config.Records.SensitivePaths != nil {
		if r, parsed := (*record).(map[string]interface{}); parsed {
			for _, path := range *config.Records.SensitivePaths {
				if fieldValue := getValueAtPath(path, r); fieldValue != nil {
					hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
					setValueAtPath(path, r, hex.EncodeToString(hash[:]))
				}
			}
		}
	}
}

func generateSurrogateKey(record *interface{}, config Config) {
	if r, parsed := (*record).(map[string]interface{}); parsed {
		h := sha256.New()
		h.Write([]byte(toString(r)))
		r["_sdc_natural_key"] = getValueAtPath(*config.Records.UniqueKeyPath, r)
		r["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
		r["_sdc_time_extracted"] = time.Now().UTC().Format(time.RFC3339)
	}
}

func ProcessRecords(records *[]interface{}, config Config) error {
	for _, record := range *records {
		generateHashedRecordsFields(&record, config)
		generateSurrogateKey(&record, config)
	}
	return nil
}
