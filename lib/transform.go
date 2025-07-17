package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/5amCurfew/xtkt/models"
	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// transformRecord applies transformations including dropping fields, hashing sensitive fields, and validating against bookmark to a record
func transformRecord(record map[string]interface{}) (map[string]interface{}, error) {
	if util.GetValueAtPath(models.Config.Records.UniqueKeyPath, record) == nil {
		return nil, fmt.Errorf("unique_key field path not found in record")
	}

	if util.IsEmpty(util.GetValueAtPath(models.Config.Records.UniqueKeyPath, record)) {
		return nil, fmt.Errorf("unique_key null or empty in record")
	}

	if models.Config.Records.DropFieldPaths != nil {
		if dropFieldsError := dropFields(record); dropFieldsError != nil {
			return nil, fmt.Errorf("error dropping fields in ProcessRecord: %v", dropFieldsError)
		}
	}

	if models.Config.Records.SensitiveFieldPaths != nil {
		if generateHashedFieldsError := generateHashedFields(record); generateHashedFieldsError != nil {
			return nil, fmt.Errorf("error generating hashed field in ProcessRecord: %v", generateHashedFieldsError)
		}
	}

	if generateSurrogateKeyFieldsError := generateSurrogateKeyFields(record); generateSurrogateKeyFieldsError != nil {
		return nil, fmt.Errorf("error generating surrogate keys in ProcessRecords: %v", generateSurrogateKeyFieldsError)
	}

	// If FULL_REFRESH is true, we skip the bookmark check
	if models.FULL_REFRESH {
		return record, nil
	}

	if keep := recordVersusBookmark(record); keep {
		return record, nil
	}

	return nil, nil
}

// dropFields drops specified fields from record
func dropFields(record map[string]interface{}) error {
	for _, path := range models.Config.Records.DropFieldPaths {
		util.DropFieldAtPath(path, record)
	}

	return nil
}

// generateHashedFields hashes specified sensitive fields of a record
func generateHashedFields(record map[string]interface{}) error {
	for _, path := range models.Config.Records.SensitiveFieldPaths {
		if fieldValue := util.GetValueAtPath(path, record); fieldValue != nil {
			hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
			util.SetValueAtPath(path, record, hex.EncodeToString(hash[:]))
		} else {
			log.WithFields(log.Fields{
				"sensitive_field_path": path,
				"_sdc_natural_key":     util.GetValueAtPath(models.Config.Records.UniqueKeyPath, record),
			}).Warn("field path not found in record for hashing (sensitive fields)")
			continue
		}
	}
	return nil
}

// generateSurrogateKeyFields generates xtkt key fields for a record
func generateSurrogateKeyFields(record map[string]interface{}) error {
	h := sha256.New()
	h.Write([]byte(util.ToString(record)))

	record["_sdc_natural_key"] = util.GetValueAtPath(models.Config.Records.UniqueKeyPath, record)
	record["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
	record["_sdc_timestamp"] = time.Now().UTC().Format(time.RFC3339)

	h.Write([]byte(util.ToString(record)))
	record["_sdc_unique_key"] = hex.EncodeToString(h.Sum(nil))

	return nil
}

// recordVersusBookmark checks the record against the stream bookmark
func recordVersusBookmark(record map[string]interface{}) bool {
	key := fmt.Sprintf("%v", record["_sdc_natural_key"])

	models.StateMutex.RLock()
	defer models.StateMutex.RUnlock()

	sk, exist := models.State.Bookmark.Latest[key]
	currentSK := record["_sdc_surrogate_key"].(string)

	if !exist {
		return true // new record
	}
	return currentSK != sk // updated if _sdc_surrogate_key has changed
}

// RecordMessage generates a message of the record
func RecordMessage(record map[string]interface{}) error {

	message := models.Message{
		Type:   "RECORD",
		Record: record,
		Stream: models.STREAM_NAME,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error CREATING RECORD MESSAGE: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))

	return nil
}
