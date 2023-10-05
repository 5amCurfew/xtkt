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
	"time"

	lib "github.com/5amCurfew/xtkt/lib"
	util "github.com/5amCurfew/xtkt/util"
	log "github.com/sirupsen/logrus"
)

var URLsParsed []string

func callAPI(config lib.Config) ([]byte, error) {
	client := http.DefaultClient

	req, err := http.NewRequest("GET", *config.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating get request: %w", err)
	}

	if *config.Rest.Auth.Required {
		switch *config.Rest.Auth.Strategy {
		case "basic":
			req.SetBasicAuth(*config.Rest.Auth.Basic.Username, *config.Rest.Auth.Basic.Password)
		case "token":
			req.Header.Add(*config.Rest.Auth.Token.Header, *config.Rest.Auth.Token.HeaderValue)
		case "oauth":
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
			accesToken := util.GetValueAtPath([]string{"access_token"}, responseMap)

			header := "Authorization"
			t := "Bearer " + accesToken.(string)

			if config.Rest.Auth.Token == nil {
				config.Rest.Auth.Token = &struct {
					Header      *string `json:"header,omitempty"`
					HeaderValue *string `json:"header_value,omitempty"`
				}{Header: &header, HeaderValue: &t}
			}

			*config.Rest.Auth.Strategy = "token"
			return callAPI(config)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}
	if resp.StatusCode >= 400 {
		statusMsg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response: %d %s", resp.StatusCode, string(statusMsg))
	}
	defer resp.Body.Close()
	URLsParsed = append(URLsParsed, *config.URL)
	return io.ReadAll(resp.Body)
}

func GenerateRestRecords(config lib.Config) ([]interface{}, error) {
	if config.Rest.Sleep != nil {
		log.Info(fmt.Sprintf(`api call sleeping %d seconds`, *config.Rest.Sleep))
		time.Sleep(time.Duration(*config.Rest.Sleep) * time.Second)
	}
	var responseMap map[string]interface{}

	log.Info(fmt.Sprintf(`page: %s`, *config.URL))
	response, err := callAPI(config)
	if err != nil {
		return nil, fmt.Errorf("error calling api: %w", err)
	}

	var responseMapRecordsPath []string

	if config.Rest.Response.RecordsPath == nil {
		responseMapRecordsPath = []string{"results"}

		var data interface{}
		if err := json.Unmarshal([]byte(response), &data); err != nil {
			// error parsing the JSON, return the original output
			return nil, fmt.Errorf("error json.unmarshal of response: %w", err)
		}

		switch d := data.(type) {
		case []interface{}:
			response, _ = json.Marshal(map[string]interface{}{
				"results": d,
			})
		case map[string]interface{}:
			response, _ = json.Marshal(map[string]interface{}{
				"results": []interface{}{d},
			})
		default:
			// the response is neither an array nor an object, but empty records_path provided
		}
	} else {
		responseMapRecordsPath = *config.Rest.Response.RecordsPath
	}

	json.Unmarshal([]byte(response), &responseMap)

	records, ok := util.GetValueAtPath(responseMapRecordsPath, responseMap).([]interface{})
	if !ok {
		return nil, fmt.Errorf("error respone map does not contain records array at path: %v", responseMapRecordsPath)
	}

	if *config.Rest.Response.Pagination {
		switch *config.Rest.Response.PaginationStrategy {

		// PAGINATED, "next"
		case "next":
			nextURL := util.GetValueAtPath(*config.Rest.Response.PaginationNextPath, responseMap)
			if nextURL == nil || nextURL == "" {
				return records, nil
			} else {
				*config.URL = nextURL.(string)

				if newRecords, err := GenerateRestRecords(config); err == nil {
					records = append(records, newRecords...)
				} else {
					return nil, fmt.Errorf("error pagination next at %s: %w", *config.URL, err)
				}
			}

		// PAGINATED, "query"
		case "query":
			if len(records) == 0 {
				return records, nil
			} else {
				parsedURL, _ := url.Parse(*config.URL)
				query := parsedURL.Query()
				query.Set(*config.Rest.Response.PaginationQuery.QueryParameter, strconv.Itoa(*config.Rest.Response.PaginationQuery.QueryValue))
				parsedURL.RawQuery = query.Encode()

				*config.URL = parsedURL.String()
				*config.Rest.Response.PaginationQuery.QueryValue = *config.Rest.Response.PaginationQuery.QueryValue + *config.Rest.Response.PaginationQuery.QueryIncrement

				if newRecords, err := GenerateRestRecords(config); err == nil {
					records = append(records, newRecords...)
				} else {
					return nil, fmt.Errorf("error pagination query at %s: %w", *config.URL, err)
				}
			}
		}
	}

	return records, nil
}
