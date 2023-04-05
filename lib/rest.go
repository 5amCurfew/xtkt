package lib

import (
	"context"
	"io"
	"net/http"

	util "github.com/5amCurfew/xtkt/util"
	oauth2 "golang.org/x/oauth2"
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
			oauthConf := &oauth2.Config{
				ClientID:     *config.Auth.Oauth.ClientID,
				ClientSecret: *config.Auth.Oauth.ClientSecret,
			}
			token := &oauth2.Token{
				AccessToken:  *config.Auth.Oauth.Token,
				RefreshToken: *config.Auth.Oauth.RefreshToken,
			}
			oauthClient := oauthConf.Client(context.Background(), token)
			resp, err := oauthClient.Do(req)
			if err != nil {
				return nil, err
			}
			URLsParsed = append(URLsParsed, *config.URL)

			defer resp.Body.Close()
			return io.ReadAll(resp.Body)
		}
	}

	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	URLsParsed = append(URLsParsed, *config.URL)

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
