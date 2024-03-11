package util

import (
	"encoding/json"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

func ToString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func WriteJSON(fileName string, state interface{}) {
	result, _ := json.Marshal(state)
	os.WriteFile(fileName, result, 0644)
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
