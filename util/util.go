package util

import (
	"fmt"
)

type Config struct {
	StreamName *string `json:"stream_name,omitempty"`
	SourceType *string `json:"source_type,omitempty"`
	URL        *string `json:"url,omitempty"`
	Auth       *struct {
		Required *bool   `json:"required,omitempty"`
		Strategy *string `json:"strategy,omitempty"`
		Basic    *struct {
			Username *string `json:"username,omitempty"`
			Password *string `json:"password,omitempty"`
		} `json:"basic,omitempty"`
		Token *struct {
			Header      *string `json:"header,omitempty"`
			HeaderValue *string `json:"header_value,omitempty"`
		} `json:"token,omitempty"`
		Oauth *struct {
			ClientID     *string `json:"client_id,omitempty"`
			ClientSecret *string `json:"client_secret,omitempty"`
			TokenURL     *string `json:"token_url,omitempty"`
			RefreshToken *string `json:"refresh_token,omitempty"`
		} `json:"oauth,omitempty"`
	} `json:"auth,omitempty"`
	Database *struct {
		Table *string `json:"table,omitempty"`
	} `json:"database,omitempty"`
	Records *struct {
		UniqueKeyPath       *[]string `json:"unique_key_path,omitempty"`
		Bookmark            *bool     `json:"bookmark,omitempty"`
		PrimaryBookmarkPath *[]string `json:"primary_bookmark_path,omitempty"`
	} `json:"records,omitempty"`
	Response *struct {
		RecordsPath        *[]string `json:"records_path,omitempty"`
		Pagination         *bool     `json:"pagination,omitempty"`
		PaginationStrategy *string   `json:"pagination_strategy,omitempty"`
		PaginationNextPath *[]string `json:"pagination_next_path,omitempty"`
		PaginationQuery    *struct {
			QueryParameter *string `json:"query_parameter,omitempty"`
			QueryValue     *int    `json:"query_value,omitempty"`
			QueryIncrement *int    `json:"query_increment,omitempty"`
		} `json:"pagination_query,omitempty"`
	} `json:"response,omitempty"`
}

func ValidateConfig(cfg Config) (bool, error) {
	if cfg.URL == nil {
		return false, fmt.Errorf("url is required in config.json")
	}
	if cfg.Auth.Required == nil {
		return false, fmt.Errorf("auth.required is required in config.json (true or false)'")
	}
	if *cfg.Auth.Required && *cfg.Auth.Strategy == "basic" && cfg.Auth.Basic == nil {
		return false, fmt.Errorf("auth.basic is required in config.json required for basic authentication")
	}
	if *cfg.Auth.Required && *cfg.Auth.Strategy == "token" && cfg.Auth.Token == nil {
		return false, fmt.Errorf("auth.token is required in config.json for token authentication")
	}
	if cfg.Response.Pagination == nil {
		return false, fmt.Errorf("response.pagination is required in config.json (true or false)")
	}
	if *cfg.Response.Pagination && cfg.Response.PaginationStrategy == nil {
		return false, fmt.Errorf("response.pagination_strategy is required in config.json when auth.pagination is true (e.g. 'next')")
	}
	if *cfg.Response.Pagination && *cfg.Response.PaginationStrategy == "next" && cfg.Response.PaginationNextPath == nil {
		return false, fmt.Errorf("response.pagination_next_path is required in config.json when auth.pagination_strategy is next")
	}
	if *cfg.Response.Pagination && *cfg.Response.PaginationStrategy == "query" && cfg.Response.PaginationQuery == nil {
		return false, fmt.Errorf("response.pagination_query is required in config.json when auth.pagination_strategy is query")
	}
	if cfg.Records == nil {
		return false, fmt.Errorf("records is required in config.json")
	}
	if cfg.Records.UniqueKeyPath == nil {
		return false, fmt.Errorf("records.unique_key_path is required in config.json")
	}
	if cfg.Records.Bookmark == nil {
		return false, fmt.Errorf("records.bookmark is required in config.json (true or false)")
	}
	if *cfg.Records.Bookmark {
		if cfg.Records.PrimaryBookmarkPath == nil {
			return false, fmt.Errorf("records.primary_bookmark_path is required in config.json when records.bookmark is true")
		}
	}

	return true, nil
}

type Record map[string]interface{}

type Message struct {
	Type               string      `json:"type"`
	Data               Record      `json:"record,omitempty"`
	Stream             string      `json:"stream,omitempty"`
	TimeExtracted      string      `json:"time_extracted,omitempty"`
	Schema             interface{} `json:"schema,omitempty"`
	Value              interface{} `json:"value,omitempty"`
	KeyProperties      []string    `json:"key_properties,omitempty"`
	BookmarkProperties []string    `json:"bookmark_properties,omitempty"`
}

func ToString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

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
