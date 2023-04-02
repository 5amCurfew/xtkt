package util

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	URL                 string   `json:"url,omitempty"`
	AuthStrategy        string   `json:"auth_strategy,omitempty"`
	AuthUsername        string   `json:"auth_username,omitempty"`
	AuthPassword        string   `json:"auth_password,omitempty"`
	AuthToken           string   `json:"auth_token,omitempty"`
	ResponseRecordsPath []string `json:"response_records_path,omitempty"`
	Pagination          bool     `json:"pagination,omitempty"`
	PaginationStrategy  string   `json:"pagination_strategy,omitempty"`
	PaginationNextPath  []string `json:"pagination_next_path,omitempty"`
	UniqueKeyPath       []string `json:"unique_key_path,omitempty"`
	Bookmark            bool     `json:"bookmark,omitempty"`
	PrimaryBookmarkPath []string `json:"primary_bookmark_path,omitempty"`
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

func GenerateStreamName(config Config) string {
	return strings.Replace(config.URL+"__"+strings.Join(config.ResponseRecordsPath, "__"), "/", "_", -1)
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
