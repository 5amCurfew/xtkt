package xtkt

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
)

func Extract(config lib.Config) error {

	start := time.Now()

	// GENERATE STATE.JSON
	if _, err := os.Stat("state.json"); err != nil {
		CreateStateJSONError := lib.CreateStateJSON(config)
		if CreateStateJSONError != nil {
			return fmt.Errorf("error CREATING STATE.JSON: %w", CreateStateJSONError)
		}
	}

	// RECORDS
	var records []interface{}
	var err error
	switch *config.SourceType {
	case "rest":
		records, err = lib.GenerateRestRecords(config)
	case "db":
		records, err = lib.GenerateDatabaseRecords(config)
	case "html":
		records, err = lib.GenerateHtmlRecords(config)
	}
	if err != nil {
		return fmt.Errorf("error CREATING RECORDS: %w", err)
	}

	lib.AddMetadata(records, config)
	if config.Records.SensitivePaths != nil {
		lib.HashRecordsFields(records, config)
	}

	// SCHEMA MESSAGE
	schema, schemaError := lib.GenerateSchema(records)
	if schemaError != nil {
		return fmt.Errorf("error CREATING SCHEMA: %w", schemaError)
	}

	schemaMessageError := lib.GenerateSchemaMessage(schema, config)
	if schemaMessageError != nil {
		return fmt.Errorf("error GENERATING SCHEMA MESSAGE: %w", schemaMessageError)
	}

	// RECORD MESSAGE(S)
	for _, record := range records {
		recordMessagesError := lib.GenerateRecordMessage(record.(map[string]interface{}), config)
		if recordMessagesError != nil {
			return fmt.Errorf("error GENERATING RECORD MESSAGE: %w", recordMessagesError)
		}
	}

	end := time.Now()

	lib.GenerateMetricInfoMessage(records, end.Sub(start), config)

	// UPDATE STATE.JSON
	if lib.UsingBookmark(config) {
		switch path := *config.Records.PrimaryBookmarkPath; {
		case reflect.DeepEqual(path, []string{"*"}):
			UpdateBookmarkPrimaryError := lib.UpdateBookmarkDetection(records, config)
			if UpdateBookmarkPrimaryError != nil {
				return fmt.Errorf("error UPDATING BOOKMARK (new-record-detection): %w", UpdateBookmarkPrimaryError)
			}
		default:
			UpdateBookmarkPrimaryError := lib.UpdateBookmarkPrimary(records, config)
			if UpdateBookmarkPrimaryError != nil {
				return fmt.Errorf("error UPDATING BOOKMARK (primary-bookmark): %w", UpdateBookmarkPrimaryError)
			}
		}
	}

	// STATE MESSAGE
	stateMessageError := lib.GenerateStateMessage()
	if stateMessageError != nil {
		return fmt.Errorf("error GENERATING STATE MESSAGE: %w", stateMessageError)
	}

	return nil
}

func ValidateJSONConfig(jsonBytes []byte) error {
	var cfg lib.Config
	err := json.Unmarshal(jsonBytes, &cfg)
	if err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	if cfg.StreamName == nil {
		return fmt.Errorf("missing required field: StreamName string")
	}
	if cfg.SourceType == nil {
		return fmt.Errorf("missing required field: SourceType string")
	}
	if cfg.URL == nil {
		return fmt.Errorf("missing required field: URL string")
	}
	if cfg.Records == nil {
		return fmt.Errorf("missing required field: Records object")
	}

	if cfg.Records != nil {
		if cfg.Records.UniqueKeyPath == nil {
			return fmt.Errorf("missing required field: Records.UniqueKeyPath []string")
		}
		if cfg.Records.Bookmark != nil && *cfg.Records.Bookmark {
			// Bookmark is true, validate that PrimaryBookmarkPath is not nil
			if cfg.Records.PrimaryBookmarkPath == nil {
				return fmt.Errorf("missing required field: Records.PrimaryBookmarkPath array")
			}
		}
	}

	if *cfg.SourceType == "database" && (cfg.Database == nil || cfg.Database.Table == nil) {
		return fmt.Errorf("missing required field: Database.Table string")
	}

	if *cfg.SourceType == "rest" && cfg.Rest != nil {
		if cfg.Rest.Auth != nil && cfg.Rest.Auth.Required != nil && *cfg.Rest.Auth.Required {
			// Auth is required, validate that the strategy field is not nil
			if cfg.Rest.Auth.Strategy == nil {
				return fmt.Errorf("missing required field: Rest.Auth.Strategy string")
			}
			if *cfg.Rest.Auth.Strategy == "basic" && cfg.Rest.Auth.Basic != nil {
				if cfg.Rest.Auth.Basic.Username == nil {
					return fmt.Errorf("missing required field: Rest.Auth.Basic.Username string")
				}
				if cfg.Rest.Auth.Basic.Password == nil {
					return fmt.Errorf("missing required field: Rest.Auth.Basic.Password string")
				}
			}
			if *cfg.Rest.Auth.Strategy == "token" && cfg.Rest.Auth.Token != nil && cfg.Rest.Auth.Token.HeaderValue == nil {
				return fmt.Errorf("missing required field: Rest.Auth.Token.HeaderValue string")
			}
			if *cfg.Rest.Auth.Strategy == "oauth" && cfg.Rest.Auth.Oauth != nil {
				if cfg.Rest.Auth.Oauth.ClientID == nil {
					return fmt.Errorf("missing required field: Rest.Auth.Oauth.ClientID string")
				}
				if cfg.Rest.Auth.Oauth.ClientSecret == nil {
					return fmt.Errorf("missing required field: Rest.Auth.Oauth.ClientSecret string")
				}
				if cfg.Rest.Auth.Oauth.RefreshToken == nil {
					return fmt.Errorf("missing required field: Rest.Auth.Oauth.RefreshToken string")
				}
				if cfg.Rest.Auth.Oauth.TokenURL == nil {
					return fmt.Errorf("missing required field: Rest.Auth.Oauth.TokenURL string")
				}
			}
		}

		if cfg.Rest.Response != nil {
			if cfg.Rest.Response.Pagination == nil {
				return fmt.Errorf("missing required field: Response.Pagination bool")
			}
			if *cfg.Rest.Response.Pagination && cfg.Rest.Response.PaginationStrategy == nil {
				return fmt.Errorf("PaginatioStrategy is a required field for response")
			}
			if *cfg.Rest.Response.Pagination && *cfg.Rest.Response.PaginationStrategy == "next" && cfg.Rest.Response.PaginationNextPath == nil {
				return fmt.Errorf("PaginationNextPath is a required field for response")
			}
			if *cfg.Rest.Response.Pagination && *cfg.Rest.Response.PaginationStrategy == "query" && cfg.Rest.Response.PaginationQuery == nil {
				return fmt.Errorf("PaginationQuery is a required field for response")
			}
		}
	}
	return nil
}
