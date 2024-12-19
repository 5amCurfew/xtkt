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

func ParseREST() {
	go func() {
		defer close(parseRecordChan)
		if err := streamRESTRecords(lib.ParsedConfig); err != nil {
			log.WithFields(log.Fields{"error": err}).Info("parseREST: streamRESTRecords failed")
		}
	}()

	for record := range parseRecordChan {
		ParsingWG.Add(1)
		go parse(record)
	}
}

func streamRESTRecords(config lib.Config) error {
	var responseMap map[string]interface{}
	responseMapRecordsPath := []string{"results"}

	for {
		log.Info(fmt.Sprintf(`page: %s`, *config.URL))
		response, err := getRequest()
		if err != nil {
			return fmt.Errorf("getRequest failed: %w", err)
		}

		var data interface{}
		if err := json.Unmarshal(response, &data); err != nil {
			return fmt.Errorf("error json.Unmarshal of response: %w", err)
		}

		switch d := data.(type) {
		case []interface{}:
			response, _ = json.Marshal(map[string]interface{}{
				"results": d,
			})
		case map[string]interface{}:
			if config.Rest.Response.RecordsPath == nil {
				response, _ = json.Marshal(map[string]interface{}{
					"results": []interface{}{d},
				})
			} else {
				response, _ = json.Marshal(data)
			}
		default:
			response, _ = json.Marshal(data)
		}

		if config.Rest.Response.RecordsPath != nil {
			responseMapRecordsPath = *config.Rest.Response.RecordsPath
		}

		if err := json.Unmarshal(response, &responseMap); err != nil {
			return fmt.Errorf("error json.Unmarshal into responseMap: %w", err)
		}

		recordsInterfaceSlice, ok := util.GetValueAtPath(responseMapRecordsPath, responseMap).([]interface{})
		if !ok {
			return fmt.Errorf("error: response map does not contain records array at path: %v", responseMapRecordsPath)
		}

		// Stream records
		for _, item := range recordsInterfaceSlice {
			if recordMap, ok := item.(map[string]interface{}); ok {
				parseRecordChan <- recordMap
			} else {
				log.WithFields(log.Fields{"item": item}).Warn("encountered non-map element in records array")
			}
		}

		// Handle pagination
		if *config.Rest.Response.Pagination {
			switch *config.Rest.Response.PaginationStrategy {
			case "next":
				nextURL := util.GetValueAtPath(*config.Rest.Response.PaginationNextPath, responseMap)
				if nextURL == nil || nextURL == "" {
					return nil
				}
				*config.URL = nextURL.(string)

			case "query":
				if len(recordsInterfaceSlice) == 0 {
					return nil
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

		} else {
			break
		}
	}

	return nil
}

// /////////////////////////////////////////////////////////
// Util
// /////////////////////////////////////////////////////////
func getRequest() ([]byte, error) {
	client := http.DefaultClient

	req, err := http.NewRequest("GET", *lib.ParsedConfig.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating get request: %w", err)
	}

	if *lib.ParsedConfig.Rest.Auth.Required {
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
			return getRequest()
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
	return io.ReadAll(resp.Body)
}

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
