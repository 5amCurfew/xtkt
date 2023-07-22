package sources

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	lib "github.com/5amCurfew/xtkt/lib"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func extractDatabaseTypeFromUrl(config lib.Config) (string, error) {
	splitUrl := strings.Split(*config.URL, "://")
	if len(splitUrl) != 2 {
		return "", fmt.Errorf("invalid db URL: %s", *config.URL)
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
	// add cases for other db types here...
	default:
		return "", fmt.Errorf("unsupported database type: %s", dbType)
	}
}

func generateQuery(config lib.Config) (string, error) {

	dbType, err := extractDatabaseTypeFromUrl(config)
	if err != nil {
		return "", fmt.Errorf("error determining DATABASE TYPE: %w", err)
	}

	state, err := lib.ParseStateJSON(config)
	if err != nil {
		return "", fmt.Errorf("error parsing STATE for bookmark value: %w", err)
	}

	value := state.Value.Bookmarks[*config.StreamName]

	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT * FROM %s", *config.Database.Table))

	// Add fields to SELECT statement
	if config.Records.PrimaryBookmarkPath != nil && value.PrimaryBookmark != "" {
		field := *config.Records.PrimaryBookmarkPath
		switch dbType {
		case "postgres", "postgresql", "sqlite":
			query.WriteString(fmt.Sprintf(` WHERE CAST("%s" AS text) > '%s'`, field[0], value.PrimaryBookmark))
		case "mysql":
			query.WriteString(fmt.Sprintf(` WHERE CAST("%s" AS char) > '%s'`, field[0], value.PrimaryBookmark))
		case "sqlserver":
			query.WriteString(fmt.Sprintf(` WHERE CAST("%s" AS varchar) > '%s'`, field[0], value.PrimaryBookmark))
		default:
			return "", fmt.Errorf("unsupported database type: %s", dbType)
		}
	}
	query.WriteString(";")
	return query.String(), nil
}

func readDatabaseRows(db *sql.DB, config lib.Config) ([]interface{}, error) {
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

func GenerateDatabaseRecords(config lib.Config) ([]interface{}, error) {
	address := *config.URL
	dbType, err := extractDatabaseTypeFromUrl(config)
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

	return records, nil
}