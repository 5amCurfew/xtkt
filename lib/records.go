package lib

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/sashabaranov/go-openai"
	log "github.com/sirupsen/logrus"
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

func generateIntelligentFields(record *interface{}, config Config) error {
	if r, parsed := (*record).(map[string]interface{}); parsed {
		for _, intellientField := range *config.Records.IntelligentFields {

			openAPIKey := os.Getenv("OPENAI_API_KEY")
			if openAPIKey == "" {
				return fmt.Errorf("error GENERATING RECORD INTELLIGENT FIELD IN generateIntelligentField: OPEN_API_KEY not found")
			}
			ctx := context.Background()
			client := openai.NewClient(openAPIKey)

			req := openai.CompletionRequest{
				Model:     "ada",
				MaxTokens: 10,
				Prompt:    *intellientField.Prefix + toString(getValueAtPath(*intellientField.FieldPath, r)) + *intellientField.Suffix,
			}

			resp, err := client.CreateCompletion(ctx, req)
			if err != nil {
				return fmt.Errorf("error GENERATING RECORD INTELLIGENT FIELD IN generateIntelligentField: %v", err)
			}

			log.Info(fmt.Sprintf(`INFO: {%s (%s): %+v, prompt: %s}`, *intellientField.IntelligentFieldName, r["_sdc_natural_key"], resp.Usage, req.Prompt))

			if len(resp.Choices) == 0 {
				r[*intellientField.IntelligentFieldName] = "ERROR_NO_VALID_RESPONSE"
			} else {
				r[*intellientField.IntelligentFieldName] = resp.Choices[0].Text
			}
		}
	}
	return nil
}

func ProcessRecords(records *[]interface{}, state *State, config Config) error {
	for _, record := range *records {

		generateHashedRecordsFieldsError := generateHashedRecordsFields(&record, config)
		if generateHashedRecordsFieldsError != nil {
			return fmt.Errorf("error GENERATING RECORD HASHED FIELD IN ProcessRecords: %v", generateHashedRecordsFieldsError)
		}
		generateSurrogateKeyError := generateSurrogateKey(&record, config)
		if generateSurrogateKeyError != nil {
			return fmt.Errorf("error GENERATING RECORD SURROGATE KEY IN ProcessRecords: %v", generateSurrogateKeyError)
		}
	}

	reduceRecordsError := reduceRecords(records, state, config)
	if reduceRecordsError != nil {
		return fmt.Errorf("error REDUCING RECORDS IN ProcessRecords: %v", reduceRecordsError)
	}

	if config.Records.IntelligentFields != nil {
		for _, record := range *records {
			generateIntelligentFieldError := generateIntelligentFields(&record, config)
			if generateIntelligentFieldError != nil {
				return fmt.Errorf("error GENERATING INTELLIGENT FIELD IN ProcessRecords: %v", generateIntelligentFieldError)
			}
		}
	}

	return nil
}
