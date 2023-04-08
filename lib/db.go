package lib

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	util "github.com/5amCurfew/xtkt/util"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func extractDbTypeFromUrl(url string) (string, error) {
	splitUrl := strings.Split(url, "://")
	if len(splitUrl) != 2 {
		return "", fmt.Errorf("invalid database URL: %s", url)
	}
	dbType := splitUrl[0]
	switch dbType {
	case "postgres", "postgresql":
		return "postgres", nil
	case "mysql":
		return "mysql", nil
	case "sqlite", "file":
		return "sqlite3", nil
	// add cases for other database types here...
	default:
		return "", fmt.Errorf("unsupported database type: %s", dbType)
	}
}

func readDatabaseRows(db *sql.DB, tableName string) ([]interface{}, error) {
	rows, err := db.Query("SELECT * FROM " + tableName + ";")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range columns {
			values[i] = new(interface{})
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			if b, ok := val.([]byte); ok {
				var v interface{}
				if err := json.Unmarshal(b, &v); err == nil {
					row[col] = v
				} else {
					row[col] = string(b)
				}
			} else {
				row[col] = val
			}
		}

		result = append(result, row)
	}

	return result, nil
}

func GenerateDatabaseRecords(config util.Config) ([]interface{}, error) {
	address := *config.URL
	dbType, _ := extractDbTypeFromUrl(*config.URL)
	if dbType == "sqlite3" {
		address = strings.Split(*config.URL, ":///")[1]
	}

	db, err := sql.Open(dbType, address)
	if err != nil {
		return nil, fmt.Errorf("error opening db: %w", err)
	}
	defer db.Close()

	records, err := readDatabaseRows(db, *config.Database.Table)
	if err != nil {
		return nil, fmt.Errorf("error reading database rows: %w", err)
	}

	return records, nil
}
