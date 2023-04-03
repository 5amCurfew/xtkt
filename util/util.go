package util

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	URL  *string `json:"url,omitempty"`
	Auth *struct {
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
	} `json:"auth,omitempty"`
	Response *struct {
		RecordsPath        *[]string `json:"records_path,omitempty"`
		Pagination         *bool     `json:"pagination,omitempty"`
		PaginationStrategy *string   `json:"pagination_strategy,omitempty"`
		PaginationNextPath *[]string `json:"pagination_next_path,omitempty"`
	} `json:"response,omitempty"`
	Records *struct {
		UniqueKeyPath       *[]string `json:"unique_key_path,omitempty"`
		Bookmark            *bool     `json:"bookmark,omitempty"`
		PrimaryBookmarkPath *[]string `json:"primary_bookmark_path,omitempty"`
	} `json:"records,omitempty"`
}

func ValidateConfig(cfg Config) error {
	if cfg.URL == nil {
		return errors.New("url is required")
	}
	if cfg.Auth.Required == nil {
		return errors.New("auth.required is required (true or false)'")
	}
	if *cfg.Auth.Required && *cfg.Auth.Strategy == "basic" && cfg.Auth.Basic == nil {
		return errors.New("auth.basic is required required for basic authentication")
	}
	if *cfg.Auth.Required && *cfg.Auth.Strategy == "token" && cfg.Auth.Token == nil {
		return errors.New("auth.token is required for token authentication")
	}
	if cfg.Response.Pagination == nil {
		return errors.New("response.pagination is required (true or false)")
	}
	if *cfg.Response.Pagination && cfg.Response.PaginationStrategy == nil {
		return errors.New("response.pagination_strategy is required when auth.pagination is true (e.g. 'next')")
	}
	if cfg.Records == nil {
		return errors.New("records is required")
	}
	if cfg.Records.UniqueKeyPath == nil {
		return errors.New("unique_key_path is required")
	}
	if cfg.Records.Bookmark == nil {
		return errors.New("bookmark is required (true or false)")
	}
	if *cfg.Records.Bookmark {
		if cfg.Records.PrimaryBookmarkPath == nil {
			return errors.New("primary_bookmark_path is required when bookmark is true")
		}
	}
	return nil
}

type Record map[string]interface{}

type Message struct {
	Type               string      `json:"type"`
	Data               Record      `json:"record,omitempty"`
	Stream             string      `json:"stream,omitempty"`
	TimeExtracted      time.Time   `json:"time_extracted,omitempty"`
	Schema             interface{} `json:"schema,omitempty"`
	Value              interface{} `json:"value,omitempty"`
	KeyProperties      []string    `json:"key_properties,omitempty"`
	BookmarkProperties []string    `json:"bookmark_properties,omitempty"`
}

func ToString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func GenerateStreamName(url string, config Config) string {
	var path []string
	if config.Response.RecordsPath == nil {
		path = []string{"results"}
	} else {
		path = *config.Response.RecordsPath
	}
	return strings.Replace(strings.Replace(url+"__"+strings.Join(path, "__"), "/", "_", -1), "https:__", "", -1)
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
