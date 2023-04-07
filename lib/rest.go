package lib

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"

	util "github.com/5amCurfew/xtkt/util"
)

var URLsParsed []string

// ///////////////////////////////////////////////////////////
// PARSE RECORDS
// ///////////////////////////////////////////////////////////
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
				}{}
				config.Auth.Token.Header = &header
				config.Auth.Token.HeaderValue = &t
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
