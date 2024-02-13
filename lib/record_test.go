package lib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDropFields(t *testing.T) {
	// Set up mock ParsedConfig
	ParsedConfig = Config{
		Records: &RecordConfig{
			UniqueKeyPath:  &[]string{"name"},
			DropFieldPaths: &[][]string{{"age"}},
		},
	}

	// Test case with no error
	record := map[string]interface{}{"name": "John", "age": 30}
	var iRecord interface{} = record
	err := dropFields(&iRecord)
	assert.NoError(t, err)

	// Test case with error
	var recordErr = "hello world"
	var iRecordErr interface{} = recordErr
	err = dropFields(&iRecordErr)
	assert.Error(t, err)
}
