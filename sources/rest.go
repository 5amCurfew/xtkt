package sources

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"

	lib "github.com/5amCurfew/xtkt/lib"
	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

// StreamRESTRecords streams records from a REST API based on the provided configuration
func StreamRESTRecords(config lib.Config) error {
	responseMapRecordsPath := []string{"results"}

	for {
		log.Info(fmt.Sprintf(`page: %s`, *config.URL))
		response, err := getRequest()
		if err != nil {
			return fmt.Errorf("getRequest failed: %w", err)
		}

		normalised, err := normaliseResponse(response, config)
		if err != nil {
			return err
		}

		if config.Rest.Response.RecordsPath != nil {
			responseMapRecordsPath = *config.Rest.Response.RecordsPath
		}

		var responseMap map[string]interface{}
		if err := json.Unmarshal(normalised, &responseMap); err != nil {
			return fmt.Errorf("error json.Unmarshal into responseMap: %w", err)
		}

		records, err := extractRecords(responseMap, responseMapRecordsPath)
		if err != nil {
			return err
		}

		for _, item := range records {
			if recordMap, ok := item.(map[string]interface{}); ok {
				lib.ExtractedChan <- recordMap
			} else {
				log.WithFields(log.Fields{"item": item}).Warn("encountered non-map element in records array")
			}
		}

		if config.Rest.Response.Pagination != nil && *config.Rest.Response.Pagination {
			if err := handlePagination(config, responseMap, records); err != nil {
				if err == errNoMorePages {
					return nil
				}
				return err
			}
		} else {
			break
		}
	}

	return nil
}

// normaliseResponse normalises the response from the REST API to a consistent format
func normaliseResponse(response []byte, config lib.Config) ([]byte, error) {
	var data interface{}
	if err := json.Unmarshal(response, &data); err != nil {
		return nil, fmt.Errorf("error json.Unmarshal of response: %w", err)
	}

	switch d := data.(type) {
	case []interface{}:
		return json.Marshal(map[string]interface{}{"results": d})
	case map[string]interface{}:
		if config.Rest.Response.RecordsPath == nil {
			return json.Marshal(map[string]interface{}{"results": []interface{}{d}})
		}
		return json.Marshal(data)
	default:
		return json.Marshal(data)
	}
}

// extractRecords extracts the records from the response map at the specified path
func extractRecords(responseMap map[string]interface{}, path []string) ([]interface{}, error) {
	records, ok := util.GetValueAtPath(path, responseMap).([]interface{})
	if !ok {
		return nil, fmt.Errorf("error: response map does not contain records array at path: %v", path)
	}
	return records, nil
}

var errNoMorePages = fmt.Errorf("no more pages")

func handlePagination(config lib.Config, responseMap map[string]interface{}, records []interface{}) error {
	switch *config.Rest.Response.PaginationStrategy {
	case "next":
		nextURL := util.GetValueAtPath(*config.Rest.Response.PaginationNextPath, responseMap)
		if nextURL == nil || nextURL == "" {
			return errNoMorePages
		}
		*config.URL = nextURL.(string)
	case "query":
		if len(records) == 0 {
			return errNoMorePages
		}
		parsedURL, err := url.Parse(*config.URL)
		if err != nil {
			return fmt.Errorf("failed to parse URL: %w", err)
		}
		query := parsedURL.Query()
		query.Set(*config.Rest.Response.PaginationQuery.QueryParameter, strconv.Itoa(*config.Rest.Response.PaginationQuery.QueryValue))
		parsedURL.RawQuery = query.Encode()
		*config.URL = parsedURL.String()
		*config.Rest.Response.PaginationQuery.QueryValue += *config.Rest.Response.PaginationQuery.QueryIncrement
	}
	return nil
}

// getRequest performs a GET request to the configured URL and handles authentication if required
func getRequest() ([]byte, error) {
	client := http.DefaultClient

	req, err := http.NewRequest("GET", *lib.ParsedConfig.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating get request: %w", err)
	}

	if *lib.ParsedConfig.Rest.Auth.Required {
		if err := setAuthHeaders(req, client); err != nil {
			return nil, err
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		statusMsg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response: %d %s", resp.StatusCode, string(statusMsg))
	}
	return io.ReadAll(resp.Body)
}

// setAuthHeaders sets the appropriate authentication headers based on the configured strategy
func setAuthHeaders(req *http.Request, client *http.Client) error {
	switch *lib.ParsedConfig.Rest.Auth.Strategy {
	case "basic":
		req.SetBasicAuth(*lib.ParsedConfig.Rest.Auth.Basic.Username, *lib.ParsedConfig.Rest.Auth.Basic.Password)
	case "token":
		req.Header.Add(*lib.ParsedConfig.Rest.Auth.Token.Header, *lib.ParsedConfig.Rest.Auth.Token.HeaderValue)
	case "oauth":
		accessToken, _ := getAccessToken(client, lib.ParsedConfig)
		header := "Authorization"
		t := "Bearer " + accessToken.(string)
		if lib.ParsedConfig.Rest.Auth.Token == nil {
			lib.ParsedConfig.Rest.Auth.Token = &struct {
				Header      *string `json:"header,omitempty"`
				HeaderValue *string `json:"header_value,omitempty"`
			}{Header: &header, HeaderValue: &t}
		}
		*lib.ParsedConfig.Rest.Auth.Strategy = "token"
		return setAuthHeaders(req, client)
	}
	return nil
}

// getAccessToken gets an access token from the configured OAuth endpoint
func getAccessToken(client *http.Client, config lib.Config) (interface{}, error) {
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	_ = writer.WriteField("client_id", *config.Rest.Auth.Oauth.ClientID)
	_ = writer.WriteField("client_secret", *config.Rest.Auth.Oauth.ClientSecret)
	_ = writer.WriteField("grant_type", "refresh_token")
	_ = writer.WriteField("refresh_token", *config.Rest.Auth.Oauth.RefreshToken)
	err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("error writer.Close(): %w", err)
	}

	authReq, err := http.NewRequest("POST", *config.Rest.Auth.Oauth.TokenURL, payload)
	if err != nil {
		return nil, fmt.Errorf("error creating auth post request: %w", err)
	}
	authReq.Header.Set("Content-Type", writer.FormDataContentType())

	oauthTokenResp, err := client.Do(authReq)
	if err != nil {
		return nil, fmt.Errorf("error auth post request: %w", err)
	}
	defer oauthTokenResp.Body.Close()

	var responseMap map[string]interface{}
	oauthResp, err := io.ReadAll(oauthTokenResp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}
	output := string(oauthResp)

	if err := json.Unmarshal([]byte(output), &responseMap); err != nil {
		return nil, fmt.Errorf("error json.unmarshal of response: %w", err)
	}
	return util.GetValueAtPath([]string{"access_token"}, responseMap), nil
}
