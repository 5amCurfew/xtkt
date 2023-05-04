package lib

import (
	"fmt"
)

type Config struct {
	StreamName *string `json:"stream_name,omitempty"`
	SourceType *string `json:"source_type,omitempty"`
	URL        *string `json:"url,omitempty"`
	Records    *struct {
		UniqueKeyPath       *[]string   `json:"unique_key_path,omitempty"`
		Bookmark            *bool       `json:"bookmark,omitempty"`
		PrimaryBookmarkPath *[]string   `json:"primary_bookmark_path,omitempty"`
		SensitivePaths      *[][]string `json:"sensitive_paths,omitempty"`
	} `json:"records,omitempty"`
	Database *struct {
		Table *string `json:"table,omitempty"`
	} `json:"db,omitempty"`
	Rest *struct {
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
			Oauth *struct {
				ClientID     *string `json:"client_id,omitempty"`
				ClientSecret *string `json:"client_secret,omitempty"`
				RefreshToken *string `json:"refresh_token,omitempty"`
				TokenURL     *string `json:"token_url,omitempty"`
			} `json:"oauth,omitempty"`
		} `json:"auth,omitempty"`
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
	} `json:"rest,omitempty"`
	Html *struct {
		ElementsPath *string `json:"elements_path,omitempty"`
		Elements     *[]struct {
			Name *string `json:"name,omitempty"`
			Path *string `json:"path,omitempty"`
		} `json:"elements,omitempty"`
	} `json:"html,omitempty"`
}

func toString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}
