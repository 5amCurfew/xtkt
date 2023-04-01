package lib

import "time"

type Config struct {
	Url                   string `json:"url"`
	Response_records_path string `json:"response_records_path"`
	Unique_key            string `json:"unique_key"`
	Bookmark              bool   `json:"bookmark"`
	Primary_bookmark      string `json:"primary_bookmark"`
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
