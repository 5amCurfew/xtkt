package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// Compile-time verification that Record implements Model interface
var _ Model = (*Record)(nil)

// Record represents a data record with transformation capabilities.
// It provides a type-safe wrapper around map[string]interface{} with
// convenient accessor methods, transformation logic, and message generation.
type Record map[string]interface{}

// Create initialises the Record from a source.
// Accepts map[string]interface{} or any value that can be converted to it.
// Expects a single parameter containing the record data.
func (r *Record) Create(source ...interface{}) error {
	if len(source) == 0 || source[0] == nil {
		return fmt.Errorf("cannot create record from nil source")
	}

	// Handle map[string]interface{} directly
	if data, ok := source[0].(map[string]interface{}); ok {
		*r = Record(data)
		return nil
	}

	// Could add other type conversions here in the future
	return fmt.Errorf("unsupported source type for record: %T", source[0])
}

// Read reads the record (record no-op)
func (r *Record) Read() error {
	// Record is loaded via Create method
	return nil
}

// Update applies transformations to the record including dropping fields,
// hashing sensitive fields, and generating surrogate keys
func (r Record) Update() error {
	if util.GetValueAtPath(Config.Records.UniqueKeyPath, r) == nil {
		return fmt.Errorf("unique_key field path not found in record")
	}

	if util.IsEmpty(util.GetValueAtPath(Config.Records.UniqueKeyPath, r)) {
		return fmt.Errorf("unique_key null or empty in record")
	}

	// Drop fields if configured
	if Config.Records.DropFieldPaths != nil {
		for _, path := range Config.Records.DropFieldPaths {
			util.DropFieldAtPath(path, r)
		}
	}

	// Hash sensitive fields if configured
	if Config.Records.SensitiveFieldPaths != nil {
		for _, path := range Config.Records.SensitiveFieldPaths {
			if fieldValue := util.GetValueAtPath(path, r); fieldValue != nil {
				hash := sha256.Sum256([]byte(fmt.Sprintf("%v", fieldValue)))
				util.SetValueAtPath(path, r, hex.EncodeToString(hash[:]))
			} else {
				log.WithFields(log.Fields{
					"sensitive_field_path": path,
					"_sdc_natural_key":     util.ToKeyString(util.GetValueAtPath(Config.Records.UniqueKeyPath, r)),
				}).Warn("field path not found in record for hashing (sensitive fields)")
			}
		}
	}

	// Generate surrogate key fields
	h := sha256.New()
	h.Write([]byte(util.ToString(r)))

	// Store natural key as its original type
	r["_sdc_natural_key"] = util.GetValueAtPath(Config.Records.UniqueKeyPath, r)
	r["_sdc_surrogate_key"] = hex.EncodeToString(h.Sum(nil))
	r["_sdc_timestamp"] = time.Now().UTC().Format(time.RFC3339)

	h.Write([]byte(util.ToString(r)))
	r["_sdc_unique_key"] = hex.EncodeToString(h.Sum(nil))

	return nil
}

// Message generates a RECORD type message and writes it to stdout
func (r Record) Message() error {
	message := Message{
		Type:   "RECORD",
		Record: r.ToMap(),
		Stream: STREAM_NAME,
	}

	messageJson, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error creating record message: %w", err)
	}

	os.Stdout.Write(messageJson)
	os.Stdout.Write([]byte("\n"))

	return nil
}

// Get retrieves a value from the record
func (r Record) Get(key string) interface{} {
	return r[key]
}

// Set sets a value in the record
func (r Record) Set(key string, value interface{}) {
	r[key] = value
}

// ToMap converts the Record back to a plain map
func (r Record) ToMap() map[string]interface{} {
	return map[string]interface{}(r)
}

// PassesBookmark checks if the record should be emitted based on the bookmark state.
// Returns true if the record is new or has been updated since the last extraction.
// Always returns true when FULL_REFRESH or DISCOVER_MODE is enabled.
func (r Record) PassesBookmark() bool {
	// Skip bookmark check if doing full refresh or discovery
	if FULL_REFRESH || DISCOVER_MODE {
		return true
	}

	// Convert natural key to string for bookmark lookup (avoiding scientific notation)
	key := util.ToKeyString(r["_sdc_natural_key"])

	StateMutex.RLock()
	defer StateMutex.RUnlock()

	entry, exist := State.Bookmark.Latest[key]
	currentSK := r["_sdc_surrogate_key"].(string)

	if !exist {
		return true // new record
	}
	return currentSK != entry.SurrogateKey // updated if _sdc_surrogate_key has changed
}
