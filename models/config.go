package models

var Config StreamConfig
var STREAM_NAME string
var FULL_REFRESH bool

type StreamConfig struct {
	StreamName     string        `json:"stream_name,omitempty"`
	SourceType     string        `json:"source_type,omitempty"`
	URL            string        `json:"url,omitempty"`
	MaxConcurrency int           `json:"max_concurrency,omitempty"`
	Records        RecordsConfig `json:"records,omitempty"`
	Rest           RestConfig    `json:"rest,omitempty"`
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
