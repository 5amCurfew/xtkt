package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"time"

	util "github.com/5amCurfew/xtkt/util"
)

func generateSurrogateKey(records []interface{}, config util.Config) {
	for _, record := range records {
		r, ok := record.(map[string]interface{})
		if !ok {
			continue
		}

		r["natural_key"] = util.GetValueAtPath(*config.Records.UniqueKeyPath, r)

		h := sha256.New()
		if keyPath := config.Records.UniqueKeyPath; keyPath != nil {
			keyValue := util.GetValueAtPath(*keyPath, r)
			h.Write([]byte(util.ToString(keyValue)))
		}
		if bookmarkPath := config.Records.PrimaryBookmarkPath; bookmarkPath != nil {
			if reflect.DeepEqual(*bookmarkPath, []string{"*"}) {
				h.Write([]byte(util.ToString(r)))
			} else {
				bookmarkValue := util.ToString(util.GetValueAtPath(*bookmarkPath, r))
				if keyPath := config.Records.UniqueKeyPath; keyPath != nil {
					keyValue := util.ToString(util.GetValueAtPath(*keyPath, r))
					h.Write([]byte(keyValue + bookmarkValue))
				} else {
					h.Write([]byte(bookmarkValue))
				}
			}
		}
		r["surrogate_key"] = hex.EncodeToString(h.Sum(nil))
	}
}

func AddMetadata(records []interface{}, config util.Config) {
	for _, record := range records {
		r, _ := record.(map[string]interface{})
		r["time_extracted"] = time.Now().Format(time.RFC3339)
	}
}
