package sources

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	lib "github.com/5amCurfew/xtkt/lib"
)

func parseJSONL(file io.Reader) ([]interface{}, error) {
	scanner := bufio.NewScanner(file)
	var records []interface{}

	for scanner.Scan() {
		line := scanner.Bytes()

		var data interface{}
		err := json.Unmarshal(line, &data)
		if err != nil {
			return nil, fmt.Errorf("error UNMARSHAL IN parseJSONL: %w", err)
		}

		records = append(records, data)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error in scanner IN parseJSONL: %w", err)
	}

	return records, nil
}

func parseCSV(file io.Reader) ([]interface{}, error) {
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error ReadAll IN parseCSV: %w", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("csv file must contain header row and at least one data row")
	}

	header := records[0]
	var result []interface{}

	for _, record := range records[1:] {
		data := make(map[string]interface{})
		for i, value := range record {
			data[header[i]] = value
		}
		result = append(result, data)
	}

	return result, nil
}

func GenerateFileRecords(config lib.Config) ([]interface{}, error) {
	file, err := os.Open(*config.URL)
	if err != nil {
		return nil, fmt.Errorf("error OPENING file in GenerateFileRecords: %w", err)
	}
	defer file.Close()

	var result []interface{}

	switch strings.Split(*config.URL, ".")[1] {
	case "jsonl":
		result, err = parseJSONL(file)
	case "csv":
		result, err = parseCSV(file)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", strings.Split(*config.URL, ".")[1])
	}

	if err != nil {
		return nil, fmt.Errorf("error PARSING file IN GenerateFileRecords: %w", err)
	}

	return result, nil
}
