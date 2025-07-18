package util

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	log "github.com/sirupsen/logrus"
)

func ToString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func WriteJSON(fileName string, data interface{}) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func GetValueAtPath(path []string, input map[string]interface{}) interface{} {
	if len(path) > 0 {
		if check, ok := input[path[0]]; !ok || check == nil {
			return nil
		}
		if len(path) == 1 {
			return input[path[0]]
		}

		key := path[0]
		path = path[1:]

		nextInput, _ := input[key].(map[string]interface{})

		return GetValueAtPath(path, nextInput)
	} else {
		return input
	}
}

func SetValueAtPath(path []string, input map[string]interface{}, value interface{}) {
	if len(path) == 1 {
		input[path[0]] = value
		return
	}

	key := path[0]
	path = path[1:]

	if _, ok := input[key]; !ok {
		input[key] = make(map[string]interface{})
	}

	SetValueAtPath(path, input[key].(map[string]interface{}), value)
}

func DropFieldAtPath(path []string, input map[string]interface{}) error {
	if len(path) == 0 {
		return nil
	}

	var currentMap = input
	for i := 0; i < len(path)-1; i++ {
		key := path[i]
		value, exists := currentMap[key]
		if !exists {
			return nil
		}

		if nestedMap, ok := value.(map[string]interface{}); ok {
			currentMap = nestedMap
		} else {
			return nil
		}
	}

	lastKey := path[len(path)-1]
	// Delete the field from the nested map if it exists
	if _, exists := currentMap[lastKey]; exists {
		delete(currentMap, lastKey)
		return nil
	} else {
		log.Warn(fmt.Sprintf("drop_field field path %s not found in record", path))
		return nil
	}
}

func IsEmpty(value interface{}) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case string:
		return v == ""
	case bool:
		return !v
	case int, int32, int64:
		return reflect.ValueOf(v).Int() == 0
	case float32, float64:
		return reflect.ValueOf(v).Float() == 0
	case []interface{}:
		return len(v) == 0
	case map[string]interface{}:
		return len(v) == 0
	default:
		return false
	}
}
