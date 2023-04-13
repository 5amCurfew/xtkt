package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"
)

func GetValueAtPath(path []string, input map[string]interface{}) interface{} {
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

		return GetValueAtPath(path, nextInput)
	} else {
		return input
	}
}

func SetValueAtPath(path []string, input map[string]interface{}, value interface{}) {
	if len(path) == 1 {
		input[path[0]] = value
		return
	}

	key := path[0]
	path = path[1:]

	if _, ok := input[key]; !ok {
		input[key] = make(map[string]interface{})
	}

	SetValueAtPath(path, input[key].(map[string]interface{}), value)
}

func generateSurrogateKey(records []interface{}, config Config) {
	for _, record := range records {
		r, ok := record.(map[string]interface{})
		if !ok {
			continue
		}

		r["natural_key"] = GetValueAtPath(*config.Records.UniqueKeyPath, r)

		h := sha256.New()
		if keyPath := config.Records.UniqueKeyPath; keyPath != nil {
			keyValue := GetValueAtPath(*keyPath, r)
			h.Write([]byte(toString(keyValue)))
		}
		if bookmarkPath := config.Records.PrimaryBookmarkPath; bookmarkPath != nil {
			if reflect.DeepEqual(*bookmarkPath, []string{"*"}) {
				h.Write([]byte(toString(r)))
			} else {
				bookmarkValue := toString(GetValueAtPath(*bookmarkPath, r))
				if keyPath := config.Records.UniqueKeyPath; keyPath != nil {
					keyValue := toString(GetValueAtPath(*keyPath, r))
					h.Write([]byte(keyValue + bookmarkValue))
				} else {
					h.Write([]byte(bookmarkValue))
				}
			}
		}
		r["surrogate_key"] = hex.EncodeToString(h.Sum(nil))
	}
}

func AddMetadata(records []interface{}, config Config) {
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		r["time_extracted"] = time.Now().Format(time.RFC3339)
	}
}

func HashRecordsFields(records []interface{}, config Config) {
	for i, record := range records {
		if rec, ok := record.(map[string]interface{}); ok {
			for _, path := range *config.Records.SensitivePaths {
				if fieldValue := GetValueAtPath(path, rec); fieldValue != nil {
					hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
					SetValueAtPath(path, rec, hex.EncodeToString(hash[:]))
				}
			}
			records[i] = rec
		}
	}
}
