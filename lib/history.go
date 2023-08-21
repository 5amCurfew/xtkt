package lib

import (
	"encoding/json"
	"os"
	"time"
)

// /////////////////////////////////////////////////////////
// HISTORY.JSON
// /////////////////////////////////////////////////////////
type ExecutionMetric struct {
	Stream            string
	ExecutionStart    time.Time
	ExecutionEnd      time.Time
	ExecutionDuration time.Duration
	RecordsExtracted  int
	RecordsProcessed  int
}

func AppendToHistory(metric ExecutionMetric) error {
	// Read the existing JSON file
	file, err := os.OpenFile("history.json", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Decode the JSON data into a slice of ExecutionMetric
	var metrics []ExecutionMetric
	err = json.NewDecoder(file).Decode(&metrics)
	if err != nil && err.Error() != "EOF" {
		return err
	}

	// Append the new metric to the slice
	metrics = append(metrics, metric)

	// Rewind the file pointer to the beginning
	file.Seek(0, 0)

	// Encode the updated metrics slice as JSON
	data, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return err
	}

	// Write the JSON data back to the file
	err = os.WriteFile("history.json", data, 0644)
	if err != nil {
		return err
	}

	return nil
}
