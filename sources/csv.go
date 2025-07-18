package sources

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	lib "github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
)

// StreamCSVRecords streams records from a CSV file
func StreamCSVRecords(config *models.StreamConfig) error {
	url := config.URL

	var reader *csv.Reader
	switch {
	case strings.HasPrefix(url, "http"):
		response, err := http.Get(url)
		if err != nil {
			return fmt.Errorf("http.Get failed: %w", err)
		}
		defer response.Body.Close()
		reader = csv.NewReader(response.Body)

	default:
		file, err := os.Open(url)
		if err != nil {
			return fmt.Errorf("os.Open failed: %w", err)
		}
		defer file.Close()
		reader = csv.NewReader(file)
	}

	// Read the header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Stream records
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading row: %w", err)
		}

		record := make(map[string]interface{})
		for i, value := range row {
			record[header[i]] = value
		}
		lib.ExtractedChan <- record
	}

	return nil
}
