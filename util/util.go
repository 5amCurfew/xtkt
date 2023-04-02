package util

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	URL  *string `json:"url,omitempty"`
	Auth struct {
		Required *bool   `json:"required,omitempty"`
		Strategy *string `json:"strategy,omitempty"`
		Username *string `json:"username,omitempty"`
		Password *string `json:"password,omitempty"`
		Token    *string `json:"token,omitempty"`
	} `json:"auth,omitempty"`
	Response struct {
		RecordsPath        *[]string `json:"records_path,omitempty"`
		Pagination         *bool     `json:"pagination,omitempty"`
		PaginationStrategy *string   `json:"pagination_strategy,omitempty"`
		PaginationPath     *[]string `json:"pagination_next_path,omitempty"`
	} `json:"response,omitempty"`
	UniqueKeyPath       *[]string `json:"unique_key,omitempty"`
	Bookmark            *bool     `json:"bookmark,omitempty"`
	PrimaryBookmarkPath *[]string `json:"primary_bookmark_path,omitempty"`
}

func ValidateConfig(cfg Config) error {
	if cfg.URL == nil {
		return errors.New("URL is required")
	}
	if cfg.Auth.Required == nil {
		return errors.New("Auth.Required must be either (true or false)'")
	}
	if *cfg.Auth.Required && *cfg.Auth.Strategy == "basic" && (cfg.Auth.Username == nil || cfg.Auth.Password == nil) {
		return errors.New("Auth.Username and Auth.Password are required for basic authentication")
	}
	if *cfg.Auth.Required && *cfg.Auth.Strategy == "token" && cfg.Auth.Token == nil {
		return errors.New("Auth.Token is required for token authentication")
	}
	if cfg.Response.Pagination == nil {
		return errors.New("Response.Pagination is required (true or false)")
	}
	if *cfg.Response.Pagination && cfg.Response.PaginationStrategy == nil {
		return errors.New("Response.PaginationStrategy is required when Pagination is true (e.g. 'next')")
	}
	if cfg.UniqueKeyPath == nil {
		return errors.New("UniqueKeyPath is required")
	}
	if *cfg.Bookmark {
		if cfg.PrimaryBookmarkPath == nil {
			return errors.New("PrimaryBookmarkPath is required when Bookmark is true")
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
