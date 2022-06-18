package tools

import (
	"encoding/json"
	"strings"
)

// 取代默认的json.Unmarshel(), 解决大正数问题。
func JSONDecode(jsondata string, target interface{}) error {
	dec := json.NewDecoder(strings.NewReader(jsondata))
	dec.UseNumber()
	return dec.Decode(&target)
}
