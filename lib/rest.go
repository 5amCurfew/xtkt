package lib

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	req, _ := http.NewRequest("GET", *config.URL, nil)

	if *config.Auth.Required {
		switch *config.Auth.Strategy {
		case "basic":
			req.SetBasicAuth(*config.Auth.Basic.Username, *config.Auth.Basic.Password)
		case "token":
			req.Header.Set(*config.Auth.Token.Header, *config.Auth.Token.HeaderValue)
		case "oauth":
			url := *config.Auth.Oauth.TokenURL

			payload := &bytes.Buffer{}
			writer := multipart.NewWriter(payload)
			_ = writer.WriteField("client_id", *config.Auth.Oauth.ClientID)
			_ = writer.WriteField("client_secret", *config.Auth.Oauth.ClientSecret)
			_ = writer.WriteField("grant_type", "refresh_token")
			_ = writer.WriteField("refresh_token", *config.Auth.Oauth.RefreshToken)
			err := writer.Close()
			if err != nil {
				fmt.Println(err)
			}

			authReq, _ := http.NewRequest("POST", url, payload)

			authReq.Header.Set("Content-Type", writer.FormDataContentType())

			oauthToken, _ := http.DefaultClient.Do(authReq)
			defer oauthToken.Body.Close()

			var responseMap map[string]interface{}
			oauthResp, _ := io.ReadAll(oauthToken.Body)
			output := string(oauthResp)

			json.Unmarshal([]byte(output), &responseMap)
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

			CallAPI(config)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	URLsParsed = append(URLsParsed, *config.URL)

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
