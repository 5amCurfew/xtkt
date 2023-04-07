package lib

import (
	"database/sql"
	"fmt"
	"reflect"

	util "github.com/5amCurfew/xtkt/util"
	_ "github.com/lib/pq"
)

func readDatabaseRows(db *sql.DB, tableName string) ([]interface{}, error) {
	// Prepare the query
	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	// Execute the query
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get the column names and types
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Build a slice of maps to hold the rows
	rowsData := make([]interface{}, 0)
	for rows.Next() {
		// Create a map to hold the row data
		row := make(map[string]interface{})

		// Create a slice of interface{} to hold the values for this row
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row into the slice of interface{}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		// Iterate over the values and column names/types to populate the map
		for i, value := range values {
			columnName := columns[i]
			columnType := columnTypes[i].ScanType()
			if columnType == nil {
				continue
			}
			columnValue := reflect.ValueOf(value).Elem().Interface()
			row[columnName] = columnValue
		}

		// Append the row to the slice of maps
		rowsData = append(rowsData, row)
	}

	return rowsData, nil
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
