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
	log "github.com/sirupsen/logrus"
)

// /////////////////////////////////////////////////////////
// PARSE
// /////////////////////////////////////////////////////////
func ParseDB() {
	defer wg.Done()

	records, err := requestDBRecords()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Info("parseDB: requestDBLRecords failed")
		return
	}

	sem := make(chan struct{}, *lib.ParsedConfig.MaxConcurrency)
	for _, record := range records[1:] {
		// "Acquire" a slot in the semaphore channel
		sem <- struct{}{}
		parsingWG.Add(1)

		go func(record interface{}) {
			defer parsingWG.Done()

			// Ensure to release the slot after the goroutine finishes
			defer func() { <-sem }()

			jsonData, _ := json.Marshal(record)
			lib.ParseRecord(jsonData, resultChan)
		}(record)
	}

	parsingWG.Wait()
}

// /////////////////////////////////////////////////////////
// REQUEST
// /////////////////////////////////////////////////////////
func requestDBRecords() ([]map[string]interface{}, error) {
	var records []map[string]interface{}

	address := *lib.ParsedConfig.URL
	dbType, err := extractDatabaseTypeFromUrl()
	if err != nil {
		return nil, fmt.Errorf("unsupported database url: %w", err)
	}

	if dbType == "sqlite3" {
		address = strings.Split(*lib.ParsedConfig.URL, ":///")[1]
	}

	db, err := sql.Open(dbType, address)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	qry, err := createQuery()
	if err != nil {
		return nil, fmt.Errorf("error generating query: %w", err)
	}

	log.Info(fmt.Sprintf("executing query %s", *lib.ParsedConfig.URL))
	rows, err := db.Query(qry)
	if err != nil {
		return nil, fmt.Errorf("error parsing select: %w", err)
	}
	log.Info(fmt.Sprintf("successful query execution %s", *lib.ParsedConfig.URL))
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error parsing columns: %w", err)
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range columns {
			values[i] = new(interface{})
		}
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("error scanning rows: %w", err)
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

		records = append(records, row)
	}

	return records, nil
}

// /////////////////////////////////////////////////////////
// REQUEST UTIL
// /////////////////////////////////////////////////////////
func extractDatabaseTypeFromUrl() (string, error) {
	splitUrl := strings.Split(*lib.ParsedConfig.URL, "://")
	if len(splitUrl) != 2 {
		return "", fmt.Errorf("invalid db URL: %s", *lib.ParsedConfig.URL)
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

func createQuery() (string, error) {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT * FROM %s", *lib.ParsedConfig.Database.Table))
	return query.String(), nil
}
