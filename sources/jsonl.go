package sources

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	lib "github.com/5amCurfew/xtkt/lib"
	"github.com/5amCurfew/xtkt/models"
	log "github.com/sirupsen/logrus"
)

// StreamJSONLRecords streams records from a JSON Lines (JSONL) file
func StreamJSONLRecords(config *models.StreamConfig) error {
	url := config.URL

	var scanner *bufio.Scanner

	switch {
	case strings.HasPrefix(url, "http"):
		response, err := http.Get(url)
		if err != nil {
			return fmt.Errorf("http.Get failed: %w", err)
		}
		defer response.Body.Close()
		scanner = bufio.NewScanner(response.Body)

	default:
		file, err := os.Open(url)
		if err != nil {
			return fmt.Errorf("os.Open failed: %w", err)
		}
		defer file.Close()
		scanner = bufio.NewScanner(file)
	}

	// Stream records
	for scanner.Scan() {
		line := scanner.Bytes()

		// Make a copy of the line data to avoid data races
		lineCopy := make([]byte, len(line))
		copy(lineCopy, line)

		record := make(map[string]interface{})
		if err := json.Unmarshal(lineCopy, &record); err != nil {
			log.WithFields(log.Fields{"error": err}).Warn("streamJSONLRecords: json.Unmarshal failed")
			continue
		}

		lib.ExtractedChan <- record
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning JSONL: %w", err)
	}

	return nil
}
