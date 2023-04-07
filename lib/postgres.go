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
