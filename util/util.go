package util

import "time"

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
	UniqueKey           string   `json:"unique_key,omitempty"`
	Bookmark            bool     `json:"bookmark,omitempty"`
	PrimaryBookmark     string   `json:"primary_bookmark,omitempty"`
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
