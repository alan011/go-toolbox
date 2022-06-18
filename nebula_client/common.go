package nebula_client

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"codeops.didachuxing.com/lordaeron/go-toolbox/tools"
)

type DataSchema interface {

	// 用于生成tag或edgeType的属性字段定义的map。仅属性字段的定义，不含vid。
	GetSchema() (string, map[string]string)

	// 用于生成可写入DB的数据
	// 注意:
	//     vertex 必须包含vid字段
	//     edge 必须包含src_vid, dst_vid两个字段。
	GetDataInSchema() map[string]interface{}
}

// 把一个golang的数据，翻译成nGQL语句中的value字符串。
//
// 除了nebula原生数据类型外，还支持两种复合数据类型（将转化为JSON字符串做nebula存储）：
//
// LIST		要求数据格式为`[]interface{}`
//
// DICT		要求数据格式为`map[string]interface{}`
func getNGQLValue(fieldDef string, val interface{}) (string, error) {
	fieldTypeS := strings.Split(strings.TrimSpace(fieldDef), " ")
	if len(fieldTypeS) == 0 {
		return "", errors.New("invalid tag field defination")
	}

	handleEmptyStr := func(val interface{}) (string, error) {
		if val == nil {
			return "", nil
		}
		valStr, ok := val.(string)
		if !ok {
			return "", errors.New("expected a string value")
		}
		return strings.TrimSpace(valStr), nil
	}

	switch fieldType := strings.ToUpper(fieldTypeS[0]); fieldType {
	case "INT":
		return fmt.Sprintf("%v", val), nil
	case "INT64":
		return fmt.Sprintf("%v", val), nil
	case "INT32":
		return fmt.Sprintf("%v", val), nil
	case "INT16":
		return fmt.Sprintf("%v", val), nil
	case "INT8":
		return fmt.Sprintf("%v", val), nil
	case "FLOAT":
		return fmt.Sprintf("%v", val), nil
	case "DOUBLE":
		return fmt.Sprintf("%v", val), nil
	case "BOOL":
		return fmt.Sprintf("%v", val), nil
	case "STRING":
		return fmt.Sprintf("\"%v\"", val), nil
	case "DATE":
		value, err := handleEmptyStr(val)
		if err != nil {
			return "", err
		}
		nVal := "NULL"
		if value != "" {
			nVal = fmt.Sprintf("date(\"%s\")", val)
		}
		return nVal, nil
	case "TIME":
		value, err := handleEmptyStr(val)
		if err != nil {
			return "", err
		}
		nVal := "NULL"
		if value != "" {
			nVal = fmt.Sprintf("time(\"%s\")", val)
		}
		return nVal, nil
	case "DATETIME":
		value, err := handleEmptyStr(val)
		if err != nil {
			return "", err
		}
		nVal := "NULL"
		if value != "" {
			nVal = fmt.Sprintf("datetime(\"%s\")", val)
		}
		return nVal, nil
	case "TIMESTAMP":
		if strVal, ok := val.(string); ok {
			return strVal, nil
		}
		expVal, ok := val.(int64)
		if !ok {
			if JNum, ok := val.(json.Number); ok {
				iVal, err := JNum.Int64()
				if err != nil {
					return "", errors.New("failed to convert json.Number to timestamp for nebula_client")
				}
				expVal = iVal
			} else {
				return "", errors.New("invalid timestamp(int64) value for nebula_client")
			}
		}
		return fmt.Sprintf("%d", expVal), nil
	case "LIST":
		strBytes, err := json.Marshal(val)
		if err != nil {
			return "", fmt.Errorf("illegal 'list' value '%s'. %s", val, err.Error())
		}
		strVal := string(strBytes)
		if strVal == "null" {
			return "'[]'", nil
		}
		return fmt.Sprintf("'%s'", strVal), nil
	case "DICT":
		strBytes, err := json.Marshal(val)
		if err != nil {
			return "", fmt.Errorf("illegal 'dict' value '%v'. %s", val, err.Error())
		}
		strVal := string(strBytes)
		strVal = strconv.Quote(strVal)
		if strVal == "null" || strVal == "map[]" {
			return "'{}'", nil
		}
		return strVal, nil
	default:
		return "", fmt.Errorf("data type '%s' not supported by this nebula client", fieldType)
	}
}

// 把一个查询结果的inString值，转换成golang中真实的inType的值。
func getRealValue(fieldDef string, valStr string) (interface{}, error) {
	fieldTypeS := strings.Split(strings.TrimSpace(fieldDef), " ")
	if len(fieldTypeS) == 0 {
		return "", errors.New("invalid tag field defination")
	}

	trim := func(str string) string {
		return strings.TrimSuffix(strings.TrimPrefix(str, "\""), "\"")
	}

	// 未定义的字段，转换为NULL值
	if valStr == "UNKNOWN_PROP" {
		valStr = "__NULL__"
	}

	fieldType := strings.ToUpper(fieldTypeS[0])
	switch fieldType {
	case "INT":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseInt(valStr, 10, 64)
	case "INT64":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseInt(valStr, 10, 64)
	case "INT32":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseInt(valStr, 10, 32)
	case "INT16":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseInt(valStr, 10, 16)
	case "INT8":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseInt(valStr, 10, 8)
	case "FLOAT":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseFloat(valStr, 32)
	case "DOUBLE":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseFloat(valStr, 64)
	case "BOOL":
		if valStr == "__NULL__" {
			return false, nil
		}
		return strconv.ParseBool(valStr)
	case "STRING":
		if valStr == "__NULL__" {
			return "", nil
		}
		return trim(valStr), nil
	case "DATE":
		if valStr == "__NULL__" {
			return "", nil
		}
		return trim(valStr), nil
	case "TIME":
		if valStr == "__NULL__" {
			return "", nil
		}
		return trim(valStr), nil
	case "DATETIME":
		if valStr == "__NULL__" {
			return "", nil
		}
		rawDate := trim(valStr)
		dateObj, err := time.Parse("2006-01-02T15:04:05.999999", rawDate)
		if err != nil {
			panic(fmt.Errorf("nebula datetime format changed. %s", err.Error()))
		}
		resultStr := dateObj.Format("2006-01-02 15:04:05")
		return resultStr, nil
	case "TIMESTAMP":
		if valStr == "__NULL__" {
			return 0, nil
		}
		return strconv.ParseInt(valStr, 10, 64)
	case "LIST":
		realList := []interface{}{}
		if valStr == "__NULL__" {
			return realList, nil
		}
		valStr = trim(valStr)
		// valBytes := []byte(valStr)
		if err := tools.JSONDecode(valStr, &realList); err != nil {
			return nil, fmt.Errorf("data in nebula is not a list, field type '%s'. %s", fieldType, err.Error())
		}
		return realList, nil
	case "DICT":
		realDict := map[string]interface{}{}
		if valStr == "__NULL__" {
			return realDict, nil
		}
		valStr = trim(valStr)
		// valBytes := []byte(valStr)
		if err := tools.JSONDecode(valStr, &realDict); err != nil {
			return nil, fmt.Errorf("data in nebula is not a dict, field type '%s'. %s", fieldType, err.Error())
		}
		return realDict, nil
	}
	return "", fmt.Errorf("data type '%s' not supported by this nebula client", fieldType)
}

func GetNebulaKeywords() []string {
	return []string{
		"GO",
		"AS",
		"TO",
		"OR",
		"AND",
		"XOR",
		"USE",
		"SET",
		"FROM",
		"WHERE",
		"MATCH",
		"INSERT",
		"YIELD",
		"RETURN",
		"DESCRIBE",
		"DESC",
		"VERTEX",
		"VERTICES",
		"EDGE",
		"EDGES",
		"UPDATE",
		"UPSERT",
		"WHEN",
		"DELETE",
		"FIND",
		"LOOKUP",
		"ALTER",
		"STEPS",
		"STEP",
		"OVER",
		"UPTO",
		"REVERSELY",
		"INDEX",
		"INDEXES",
		"REBUILD",
		"BOOL",
		"INT8",
		"INT16",
		"INT32",
		"INT64",
		"INT",
		"FLOAT",
		"DOUBLE",
		"STRING",
		"FIXED_STRING",
		"TIMESTAMP",
		"DATE",
		"TIME",
		"DATETIME",
		"TAG",
		"TAGS",
		"UNION",
		"INTERSECT",
		"MINUS",
		"NO",
		"OVERWRITE",
		"SHOW",
		"ADD",
		"CREATE",
		"DROP",
		"REMOVE",
		"IF",
		"NOT",
		"EXISTS",
		"WITH",
		"CHANGE",
		"GRANT",
		"REVOKE",
		"ON",
		"BY",
		"IN",
		"NOT_IN",
		"DOWNLOAD",
		"GET",
		"OF",
		"ORDER",
		"INGEST",
		"COMPACT",
		"FLUSH",
		"SUBMIT",
		"ASC",
		"ASCENDING",
		"DESCENDING",
		"DISTINCT",
		"FETCH",
		"PROP",
		"BALANCE",
		"STOP",
		"LIMIT",
		"OFFSET",
		"IS",
		"NULL",
		"RECOVER",
		"EXPLAIN",
		"PROFILE",
		"FORMAT",
		"CASE",
	}
}

func IsNebulaKeyword(word string) bool {
	return tools.IsStrInSlice(strings.ToUpper(word), GetNebulaKeywords())
}

// stored as a numnber.
func NumberTypes() []string {
	return []string{
		"INT",
		"INT64",
		"INT32",
		"INT16",
		"INT8",
		"FLOAT",
		"DOUBLE",
		"BOOL",
		"TIMESTAMP",
	}
}

func TimeTypes() []string {
	return []string{
		"DATE",
		"TIME",
		"DATETIME",
	}
}

func ComplexTypes() []string {
	return []string{
		"LIST",
		"DICT",
	}
}

// stored as a string
func SearchableTypes() []string {
	return []string{
		"STRING",
		"IPADDR",
		"LIST",
		"DICT",
	}
}
