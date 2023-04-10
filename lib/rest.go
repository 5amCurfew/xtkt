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

	util "github.com/5amCurfew/xtkt/util"
)

var URLsParsed []string

func CallAPI(config util.Config) ([]byte, error) {
	client := http.DefaultClient

	req, err := http.NewRequest("GET", *config.URL, nil)
	if err != nil {
		return nil, err
	}

	if *config.Auth.Required {
		switch *config.Auth.Strategy {
		case "basic":
			req.SetBasicAuth(*config.Auth.Basic.Username, *config.Auth.Basic.Password)
		case "token":
			req.Header.Add(*config.Auth.Token.Header, *config.Auth.Token.HeaderValue)
		case "oauth":
			payload := &bytes.Buffer{}
			writer := multipart.NewWriter(payload)
			_ = writer.WriteField("client_id", *config.Auth.Oauth.ClientID)
			_ = writer.WriteField("client_secret", *config.Auth.Oauth.ClientSecret)
			_ = writer.WriteField("grant_type", "refresh_token")
			_ = writer.WriteField("refresh_token", *config.Auth.Oauth.RefreshToken)
			err := writer.Close()
			if err != nil {
				return nil, err
			}

			url := *config.Auth.Oauth.TokenURL
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
			accesToken := util.GetValueAtPath([]string{"access_token"}, responseMap)

			header := "Authorization"
			t := "Bearer " + accesToken.(string)

			if config.Auth.Token == nil {
				config.Auth.Token = &struct {
					Header      *string `json:"header,omitempty"`
					HeaderValue *string `json:"header_value,omitempty"`
				}{Header: &header, HeaderValue: &t}
			}

			*config.Auth.Strategy = "token"
			return CallAPI(config)
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

func GenerateRestRecords(config util.Config) []interface{} {
	var responseMap map[string]interface{}

	response, err := CallAPI(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error calling API: %v\n", err)
		os.Exit(1)
	}

	var responseMapRecordsPath []string

	if config.Response.RecordsPath == nil {
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
		responseMapRecordsPath = *config.Response.RecordsPath
	}

	json.Unmarshal([]byte(response), &responseMap)

	records, ok := util.GetValueAtPath(responseMapRecordsPath, responseMap).([]interface{})
	if !ok {
		fmt.Fprint(os.Stderr, "Error: records is not an array\n")
		os.Exit(1)
	}

	if *config.Response.Pagination {
		switch *config.Response.PaginationStrategy {

		// PAGINATED, "next"
		case "next":
			nextURL := util.GetValueAtPath(*config.Response.PaginationNextPath, responseMap)
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
				query.Set("page", strconv.Itoa(*config.Response.PaginationQuery.QueryValue))
				parsedURL.RawQuery = query.Encode()

				*config.URL = parsedURL.String()
				*config.Response.PaginationQuery.QueryValue = *config.Response.PaginationQuery.QueryValue + *config.Response.PaginationQuery.QueryIncrement
				records = append(records, GenerateRestRecords(config)...)
			}
		}
	}

	generateSurrogateKey(records, config)
	return records
}
