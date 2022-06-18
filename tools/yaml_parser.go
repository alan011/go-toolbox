package tools

import (
	"os"

	"gopkg.in/yaml.v2"
)

func ParseYaml(filePath string, receiver interface{}) error {
	// To read the whole yaml file.
	data_bytes, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// To decode yaml content-bytes.
	return yaml.Unmarshal(data_bytes, receiver)
}
