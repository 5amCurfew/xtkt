package util

import "time"

type Config struct {
	URL                 string `json:"url"`
	ResponseRecordsPath string `json:"response_records_path"`
	Paginated           bool   `json:"paginated"`
	PaginationStrategy  string `json:"pagination_strategy,omitempty"`
	PaginationNextPath  string `json:"pagination_next_path,omitempty"`
	UniqueKey           string `json:"unique_key"`
	Bookmark            bool   `json:"bookmark"`
	PrimaryBookmark     string `json:"primary_bookmark"`
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
