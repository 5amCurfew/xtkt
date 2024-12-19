package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/xeipuuv/gojsonschema"

	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// Transform record
func ProcessRecord(record *interface{}) (*interface{}, error) {

	r, parsed := (*record).(map[string]interface{})
	if !parsed {
		return nil, fmt.Errorf("error parsing record as map[string]interface{} in ProcessRecord")
	}

	if ParsedConfig.Records.DropFieldPaths != nil {
		if dropFieldsError := dropFields(r); dropFieldsError != nil {
			return nil, fmt.Errorf("error dropping fields in ProcessRecord: %v", dropFieldsError)
		}
	}

	if ParsedConfig.Records.SensitiveFieldPaths != nil {
		if generateHashedFieldsError := generateHashedFields(r); generateHashedFieldsError != nil {
			return nil, fmt.Errorf("error generating hashed field in ProcessRecord: %v", generateHashedFieldsError)
		}
	}

	if generateSurrogateKeyFieldsError := generateSurrogateKeyFields(r); generateSurrogateKeyFieldsError != nil {
		return nil, fmt.Errorf("error generating surrogate keys in ProcessRecords: %v", generateSurrogateKeyFieldsError)
	}

	if keep := recordVersusBookmark(r); keep {
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

// Transform: hash specified fields in record
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

// Transform: generate surrogate keys for record
func generateSurrogateKeyFields(record map[string]interface{}) error {
	h := sha256.New()
	h.Write([]byte(util.ToString(record)))
	if util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, record) != nil {
		record["_sdc_natural_key"] = util.GetValueAtPath(*ParsedConfig.Records.UniqueKeyPath, record)
	} else {
		log.WithFields(log.Fields{
			"unique_key_path": *ParsedConfig.Records.UniqueKeyPath,
		}).Warn("unique_key field path not found in record")
	}
	record["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
	record["_sdc_time_extracted"] = time.Now().UTC().Format(time.RFC3339)

	return nil
}

// Hold record against bookmark
func recordVersusBookmark(record map[string]interface{}) bool {
	key := record["_sdc_surrogate_key"].(string)

	stateMutex.Lock() // Prevent concurrent read/writes to state
	defer stateMutex.Unlock()

	_, found := ParsedState.Value.Bookmarks[*ParsedConfig.StreamName].Bookmark[key]
	return !found // "keep" (true) if not found
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
