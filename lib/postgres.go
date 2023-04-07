package lib

import (
	"database/sql"
	"encoding/json"
	"fmt"

	util "github.com/5amCurfew/xtkt/util"
	_ "github.com/lib/pq"
)

func readDatabaseRows(db *sql.DB, tableName string) ([]interface{}, error) {
	rows, err := db.Query("SELECT * FROM " + tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range cols {
			valuePtrs[i] = &values[i]
		}
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range cols {
			val := values[i]
			if b, ok := val.([]byte); ok {
				// If the value is a byte slice, try to parse it as JSON.
				// If parsing fails, fall back to treating it as a string.
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

func GenerateDatabaseRecords(config util.Config) []interface{} {

	db, err := sql.Open("postgres", *config.URL)
	if err != nil {
		fmt.Println("error opening db:", err.Error())
	}
	defer db.Close()

	// Call the RowsToJSON function with the table name
	records, _ := readDatabaseRows(db, *config.Database.Table)

	return records

}
