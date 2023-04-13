package lib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

var URLsParsed []string

func callAPI(config Config) ([]byte, error) {
	client := http.DefaultClient

	req, err := http.NewRequest("GET", *config.URL, nil)
	if err != nil {
		return nil, err
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
				return nil, err
			}

			url := *config.Rest.Auth.Oauth.TokenURL
			authReq, err := http.NewRequest("POST", url, payload)
			if err != nil {
				return nil, err
			}
			authReq.Header.Set("Content-Type", writer.FormDataContentType())

			oauthTokenResp, err := client.Do(authReq)
			if err != nil {
				return nil, err
			}
			defer oauthTokenResp.Body.Close()

			var responseMap map[string]interface{}
			oauthResp, err := io.ReadAll(oauthTokenResp.Body)
			if err != nil {
				return nil, err
			}
			output := string(oauthResp)

			if err := json.Unmarshal([]byte(output), &responseMap); err != nil {
				return nil, err
			}
			accesToken := getValueAtPath([]string{"access_token"}, responseMap)

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
		return nil, err
	}
	defer resp.Body.Close()
	URLsParsed = append(URLsParsed, *config.URL)
	return io.ReadAll(resp.Body)
}

func GenerateRestRecords(config Config) []interface{} {
	var responseMap map[string]interface{}

	response, err := callAPI(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error calling API: %v\n", err)
		os.Exit(1)
	}

	var responseMapRecordsPath []string

	if config.Rest.Response.RecordsPath == nil {
		responseMapRecordsPath = []string{"results"}

		var data interface{}
		if err := json.Unmarshal([]byte(response), &data); err != nil {
			// error parsing the JSON, return the original output
			return nil
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

	records, ok := getValueAtPath(responseMapRecordsPath, responseMap).([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: confirm your config.json is aligned with the API reponse")
		os.Exit(1)
	}

	if *config.Rest.Response.Pagination {
		switch *config.Rest.Response.PaginationStrategy {

		// PAGINATED, "next"
		case "next":
			nextURL := getValueAtPath(*config.Rest.Response.PaginationNextPath, responseMap)
			if nextURL == nil || nextURL == "" {
				generateSurrogateKey(records, config)
				return records
			} else {
				*config.URL = nextURL.(string)
				records = append(records, GenerateRestRecords(config)...)
			}

		// PAGINATED, "query"
		case "query":
			if len(records) == 0 {
				generateSurrogateKey(records, config)
				return records
			} else {
				parsedURL, _ := url.Parse(*config.URL)
				query := parsedURL.Query()
				query.Set("page", strconv.Itoa(*config.Rest.Response.PaginationQuery.QueryValue))
				parsedURL.RawQuery = query.Encode()

				*config.URL = parsedURL.String()
				*config.Rest.Response.PaginationQuery.QueryValue = *config.Rest.Response.PaginationQuery.QueryValue + *config.Rest.Response.PaginationQuery.QueryIncrement
				records = append(records, GenerateRestRecords(config)...)
			}
		}
	}

	generateSurrogateKey(records, config)
	return records
}
