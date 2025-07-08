package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/xeipuuv/gojsonschema"

	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// Transform record including dropping fields, hashing sensitive fields, and validating against bookmark
func ProcessRecord(record map[string]interface{}) (map[string]interface{}, error) {
	if util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, record) == nil {
		return nil, fmt.Errorf("unique_key field path not found in record")
	}

	if util.IsEmpty(util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, record)) {
		return nil, fmt.Errorf("unique_key null or empty in record")
	}

	if ParsedConfig.Records.DropFieldPaths != nil {
		if dropFieldsError := dropFields(record); dropFieldsError != nil {
			return nil, fmt.Errorf("error dropping fields in ProcessRecord: %v", dropFieldsError)
		}
	}

	if ParsedConfig.Records.SensitiveFieldPaths != nil {
		if generateHashedFieldsError := generateHashedFields(record); generateHashedFieldsError != nil {
			return nil, fmt.Errorf("error generating hashed field in ProcessRecord: %v", generateHashedFieldsError)
		}
	}

	if generateSurrogateKeyFieldsError := generateSurrogateKeyFields(record); generateSurrogateKeyFieldsError != nil {
		return nil, fmt.Errorf("error generating surrogate keys in ProcessRecords: %v", generateSurrogateKeyFieldsError)
	}

	if keep := recordVersusBookmark(record); keep {
		return record, nil
	}

	return nil, nil
}

// Transform: drop specified fields from record
func dropFields(record map[string]interface{}) error {
	for _, path := range *ParsedConfig.Records.DropFieldPaths {
		util.DropFieldAtPath(path, record)
	}

	return nil
}

// Transform: hash specified sensitive fields in record
func generateHashedFields(record map[string]interface{}) error {
	for _, path := range *ParsedConfig.Records.SensitiveFieldPaths {
		if fieldValue := util.GetValueAtPath(path, record); fieldValue != nil {
			hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
			util.SetValueAtPath(path, record, hex.EncodeToString(hash[:]))
		} else {
			log.WithFields(log.Fields{
				"sensitive_field_path": path,
				"_sdc_natural_key":     util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, record),
			}).Warn("field path not found in record for hashing (sensitive fields)")
			continue
		}
	}
	return nil
}

// Transform: generate surrogate keys for record following transformations
func generateSurrogateKeyFields(record map[string]interface{}) error {
	h := sha256.New()
	h.Write([]byte(util.ToString(record)))

	record["_sdc_natural_key"] = util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, record)
	record["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
	record["_sdc_timestamp"] = time.Now().UTC().Format(time.RFC3339)

	return nil
}

// Hold record against bookmark
func recordVersusBookmark(record map[string]interface{}) bool {
	key := record["_sdc_surrogate_key"].(string)

	stateMutex.RLock() // Shared lock for read-only access
	defer stateMutex.RUnlock()

	_, foundInBookmark := ParsedState.Bookmark.Seen[key]
	return !foundInBookmark // "keep" (true if not found)
}

// Validate record against Catalog
func ValidateRecordSchema(record map[string]interface{}, schema map[string]interface{}) (bool, error) {
	// Convert schema map to a JSON string
	schemaLoader := gojsonschema.NewGoLoader(schema)
	recordLoader := gojsonschema.NewGoLoader(record)

	// Validate the record against the schema
	result, _ := gojsonschema.Validate(schemaLoader, recordLoader)

	// Check if validation was successful
	if result.Valid() {
		return true, nil
	}

	return false, fmt.Errorf("%s", result.Errors())
}

// ProduceRecordMessage generates a message with the schema of the record
func ProduceRecordMessage(record interface{}) error {
	r, parsed := record.(map[string]interface{})
	if !parsed {
		return fmt.Errorf("error PARSING RECORD IN ProduceRecordMessage: %v", r)
	}

	message := Message{
		Type:   "RECORD",
		Record: r,
		Stream: *ParsedConfig.StreamName,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING RECORD MESSAGE: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))

	return nil
}
