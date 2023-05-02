package lib

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func extractDbTypeFromUrl(config Config) (string, error) {
	splitUrl := strings.Split(*config.URL, "://")
	if len(splitUrl) != 2 {
		return "", fmt.Errorf("invalid database URL: %s", *config.URL)
	}
	dbType := splitUrl[0]
	switch dbType {
	case "postgres", "postgresql":
		return "postgres", nil
	case "mysql":
		return "mysql", nil
	case "sqlite", "file":
		return "sqlite3", nil
	case "sqlserver":
		return "mssql", nil
	// add cases for other database types here...
	default:
		return "", fmt.Errorf("unsupported database type: %s", dbType)
	}
}

func generateQuery(config Config) (string, error) {

	dbType, _ := extractDbTypeFromUrl(config)
	value, err := readBookmark(config)
	if err != nil {
		return "", fmt.Errorf("error generating query with bookmark value: %w", err)
	}

	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT * FROM %s", *config.Database.Table))

	// Add fields to SELECT statement
	if config.Records.PrimaryBookmarkPath != nil && value["primary_bookmark"] != "" {
		field := *config.Records.PrimaryBookmarkPath
		switch dbType {
		case "postgres", "postgresql", "sqlite":
			query.WriteString(fmt.Sprintf(` WHERE CAST("%s" AS text) > '%s'`, field[0], value["primary_bookmark"]))
		case "mysql":
			query.WriteString(fmt.Sprintf(` WHERE CAST("%s" AS char) > '%s'`, field[0], value["primary_bookmark"]))
		case "sqlserver":
			query.WriteString(fmt.Sprintf(` WHERE CAST("%s" AS varchar) > '%s'`, field[0], value["primary_bookmark"]))
		default:
			return "", fmt.Errorf("unsupported database type: %s", dbType)
		}
	}
	query.WriteString(";")
	return query.String(), nil
}

func readDatabaseRows(db *sql.DB, config Config) ([]interface{}, error) {
	qry, err := generateQuery(config)
	if err != nil {
		return nil, fmt.Errorf("error generating QUERY: %w", err)
	}

	rows, err := db.Query(qry)
	if err != nil {
		return nil, fmt.Errorf("error parsing SELECT: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error parsing COLUMNS: %w", err)
	}

	result := make([]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range columns {
			values[i] = new(interface{})
		}
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("error ROWS SCAN: %w", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			switch v := val.(type) {
			case []byte:
				var r interface{}
				if err := json.Unmarshal(v, &r); err == nil {
					row[col] = r
				} else {
					row[col] = string(v)
				}
			case nil:
				row[col] = nil
			default:
				row[col] = v
			}
		}

		result = append(result, row)
	}

	return result, nil
}

func GenerateDatabaseRecords(config Config) ([]interface{}, error) {
	address := *config.URL
	dbType, err := extractDbTypeFromUrl(config)
	if err != nil {
		return nil, fmt.Errorf("unsupported database URL: %w", err)
	}

	if dbType == "sqlite3" {
		address = strings.Split(*config.URL, ":///")[1]
	}

	db, err := sql.Open(dbType, address)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}
	defer db.Close()

	records, err := readDatabaseRows(db, config)
	if err != nil {
		return nil, fmt.Errorf("error reading database rows: %w", err)
	}

	generateSurrogateKey(records, config)
	return records, nil
}
