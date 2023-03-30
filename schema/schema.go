package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func GenerateSurrogateKey(c util.Config, records []interface{}) {
	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			data := c.Unique_key + r[c.Primary_bookmark].(string)
			h := sha256.New()
			h.Write([]byte(data))

			hashBytes := h.Sum(nil)

			r["surrogate_key"] = hex.EncodeToString(hashBytes)
		}
	}
}

// ///////////////////////////////////////////////////////////
// GENERATE JSON SCHEMA
// ///////////////////////////////////////////////////////////
func GenerateSchema(records []interface{}) map[string]interface{} {

	schema := make(map[string]interface{})
	properties := make(map[string]interface{})

	if len(records) > 0 {
		for _, record := range records {
			r, _ := record.(map[string]interface{})
			for key, value := range r {
				if _, exists := properties[key]; !exists {
					properties[key] = make(map[string]interface{})
					switch value.(type) {
					case bool:
						properties[key].(map[string]interface{})["type"] = "boolean"
					case int:
						properties[key].(map[string]interface{})["type"] = "integer"
					case float64:
						properties[key].(map[string]interface{})["type"] = "number"
					case map[string]interface{}:
						subProps := GenerateSchema([]interface{}{value})
						properties[key].(map[string]interface{})["type"] = "object"
						properties[key].(map[string]interface{})["properties"] = subProps["properties"]
					case []interface{}:
						properties[key].(map[string]interface{})["type"] = "array"
					case nil:
						properties[key].(map[string]interface{})["type"] = "null"
					case string:
						if _, err := time.Parse(time.RFC3339, value.(string)); err == nil {
							properties[key].(map[string]interface{})["type"] = "timestamp"
							break
						} else if _, err := time.Parse("2006-01-02", value.(string)); err == nil {
							properties[key].(map[string]interface{})["type"] = "date"
							break
						} else {
							properties[key].(map[string]interface{})["type"] = "string"
						}
					}
				}
			}
		}
	}

	schema["properties"] = properties
	schema["type"] = "object"
	return schema
}
