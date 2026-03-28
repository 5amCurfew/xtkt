package models

import (
	"encoding/json"
	"fmt"
	"os"
)

// Compile-time verification that StreamConfig implements Model interface
var _ Model = (*StreamConfig)(nil)

// StreamConfig represents the configuration for a data stream.
// It defines the source type, connection details, authentication, and record processing rules.
type StreamConfig struct {
	StreamName     string        `json:"stream_name,omitempty"`
	SourceType     string        `json:"source_type,omitempty"`
	URL            string        `json:"url,omitempty"`
	MaxConcurrency int           `json:"max_concurrency,omitempty"`
	Records        RecordsConfig `json:"records,omitempty"`
	Rest           RestConfig    `json:"rest,omitempty"`
}

var Config StreamConfig
var STREAM_NAME string
var FULL_REFRESH bool
var DISCOVER_MODE bool

// Create loads the StreamConfig from a JSON file
// Expects a single string parameter containing the file path
func (c *StreamConfig) Create(source ...interface{}) error {
	if len(source) == 0 {
		return fmt.Errorf("config file path required")
	}
	filePath, ok := source[0].(string)
	if !ok {
		return fmt.Errorf("config file path must be string, got %T", source[0])
	}
	configData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}

	if err := json.Unmarshal(configData, c); err != nil {
		return fmt.Errorf("error unmarshaling config json: %w", err)
	}

	return nil
}

// Read reads the configuration (JSON file is loaded via Create, so this is a no-op)
func (c *StreamConfig) Read() error {
	// Config is loaded via Create method
	return nil
}

// Update updates the configuration (no-op for config)
func (c *StreamConfig) Update() error {
	// Config is read-only after initial load
	return nil
}

// Message generates a configuration message (no-op for config)
func (c *StreamConfig) Message() error {
	// Config doesn't generate messages in the current pipeline
	return nil
}

type RecordsConfig struct {
	UniqueKeyPath       []string   `json:"unique_key_path,omitempty"`
	DropFieldPaths      [][]string `json:"drop_field_paths,omitempty"`
	SensitiveFieldPaths [][]string `json:"sensitive_field_paths,omitempty"`
}

type BasicAuthConfig struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type TokenAuthConfig struct {
	Header      string `json:"header,omitempty"`
	HeaderValue string `json:"header_value,omitempty"`
}

type OAuthConfig struct {
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	TokenURL     string `json:"token_url,omitempty"`
}

type AuthConfig struct {
	Required bool            `json:"required,omitempty"`
	Strategy string          `json:"strategy,omitempty"`
	Basic    BasicAuthConfig `json:"basic,omitempty"`
	Token    TokenAuthConfig `json:"token,omitempty"`
	OAuth    OAuthConfig     `json:"oauth,omitempty"`
}

type PaginationQueryConfig struct {
	QueryParameter string `json:"query_parameter,omitempty"`
	QueryValue     int    `json:"query_value,omitempty"`
	QueryIncrement int    `json:"query_increment,omitempty"`
}

type ResponseConfig struct {
	RecordsPath        []string              `json:"records_path,omitempty"`
	Pagination         bool                  `json:"pagination,omitempty"`
	PaginationStrategy string                `json:"pagination_strategy,omitempty"`
	PaginationNextPath []string              `json:"pagination_next_path,omitempty"`
	PaginationQuery    PaginationQueryConfig `json:"pagination_query,omitempty"`
}

type RestConfig struct {
	Auth     AuthConfig     `json:"auth,omitempty"`
	Response ResponseConfig `json:"response,omitempty"`
}
