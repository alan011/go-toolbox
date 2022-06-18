package ginstarter

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	slog "codeops.didachuxing.com/lordaeron/go-toolbox/simplelog"
)

// 第一层map的key为字段名称
//
// 第二层map的key为校验配置属性，支持以下属性：
//
// `required`		bool类型，true表示该字段必须提供。
//
// `type`			string类型。
//                  目前支持的数据类型： 'int', 'float', 'string', 'bool', 'ipaddr', 'datetime', 'timestamp', 'list', 'dict', 'jsonlist', 'jsondict',
//					分别对应golang中的数据类型，如下：
//						float 对应float64
//						timestamp 对应int64
//						time, datetime, ipaddr, jsonlist, jsondict 对应string，其默认校验规则不同。jsondict的key必须是string类型。
//						list 对应[]interface{}
//						dict 对应map[string]interface{}
//					其他跟golang内置类型一一对应。
//
// `min`			int or float64类型。表示数字的最小值。支持类型: 'int', 'float'.
//
// `max`			int or float64类型。表示数字的最大值。支持类型: 'int', 'float'.
//
// `regexp`			int类型。表示字符串的正则匹配要求。支持类型: 'string'.
//
// `trim_space`		bool类型。表示需要自动移除首尾的空字符。支持类型: 'string'.
//
// `not_empty`		bool类型，true表示要求数据不可为空。对于string，空白字符也被认为是空。支持类型: 'string'.
//
// `minlen`			int类型。表示数据的最小长度。支持类型：'string', 'list', 'dict'.
//
// `maxlen`			int类型。表示数据的的最大长度。支持类型：'string', 'list', 'dict'.
//
// `choices`        []interface{}类型, 表示数据只能在此选项范围之类。支持类型: 'int', 'float', 'string', 'ipaddr', 'datetime', 'timestamp'.
//                  元素支持三种数据格式：
//                    - 直接是选项value
//                    - map{string}interface{}
//                    - map[interface{}]interface{}
//                  如果是map，则必须包含'value'这个key。
//					注意：有了choices约束后，其他值类约束将失效。故，定义validator时注意不要写无用代码。
//
// `auto_convert`	bool类型，true表示需要将字符串数据自动转化为对应的type类型。要求待校验数据是个字符串。支持类型： 'int', 'float', 'bool', 'timestamp'.
//
// 每种属性还支持 `<prop>_errmsg`的属性，用于该项检查不通过时，定制错误信息。
//
// type必须提供，其他皆为可选。
//
// 复合型数据('list', 'dict', 'jsonlist', 'jsondict')，嵌套校验属约束：
//
// `item_type`		string类型。'list'专用约束。表示list元素的类型要求，类型必须是validator所支持的数据类型，可以是复合型数据。
//                  若不提供此约束，表示list类型为任意类型。即不做interface{}底层type校验。
//
// `item_<min, max, minlen, ...>`
//					'list'专用约束。表示其他各种非复合类型的数据约束，同样支持xxx_errmsg。
// `key_<minlen, maxlen, regexp, not_empty, trim_space, choices>`
//					'dict'专用约束。约束其固定为string类型的key。
//
// `value_type`		'dict'专用约束。表示dict的value的类型要求，类型必须是validator所支持的数据类型，可以是复合型数据。
//
// `value_<min, max, minlen, ...>`
// 					'dict'专用约束。表示其他各种非复合类型的数据约束，用于嵌套校验，同样支持xxx_errmsg。
// `dict_format`    'dict'专用约束。用于key为固定值的情况。
//                  注意：
//                    - 启用format约束后，会忽略掉key，value校验；也会忽略dict本身的minlen，maxlen约束。
//                    - 这跟key定义了choices约束类似，但不同之处在于，format模式的val可以定制，而key choices模式中val是统一约束。
//                    - format中的val支持required选项，用于约束字典数据的此key/val必须提供。不在format中定义的字段，将被直接丢弃。

type DataValidator struct {
	Validator   map[string]map[string]interface{}
	FixedFields bool // true表示，不在Validator中定义的字段将被丢弃。
}

// 对于本身就是字符串的类型(string, time, datetime, ipaddr)，会移除首位的空字符。
func convertStrVal(fType string, value string) (interface{}, error) {
	valStr := strings.TrimSpace(value)
	switch fType {
	case "int":
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return 0, err
		}
		return int(val), nil
	case "float":
		return strconv.ParseFloat(valStr, 64)
	case "string":
		return valStr, nil
	case "bool":
		return strconv.ParseBool(valStr)
	case "datetime":
		return valStr, nil
	case "ipaddr":
		return valStr, nil
	case "timestamp":
		// return strconv.ParseFloat(valStr, 64)
		return strconv.ParseInt(valStr, 10, 64)
	}
	return value, fmt.Errorf("type '%s' not supported", fType)
}

func (v *DataValidator) typeCheckingDispatch(data map[string]interface{}, field string, fType string, fieldCheck map[string]interface{}) error {
	// 分类型校验数据
	var errmsg error
	switch fType {
	case "int":
		errmsg = v.checkInt(data, field, fieldCheck)
	case "float":
		errmsg = v.checkFloat(data, field, fieldCheck)
	case "bool":
		errmsg = v.checkBool(data, field, fieldCheck)
	case "string":
		errmsg = v.checkString(data, field, fieldCheck)
	case "ipaddr":
		errmsg = v.checkIpaddr(data, field, fieldCheck)
	case "datetime":
		errmsg = v.checkDatetime(data, field, fieldCheck)
	case "timestamp":
		errmsg = v.checkTimestamp(data, field, fieldCheck)
	case "list":
		errmsg = v.checkList(data, field, fieldCheck)
	case "dict":
		errmsg = v.checkDict(data, field, fieldCheck)
	case "jsonlist":
		errmsg = v.checkJsonlist(data, field, fieldCheck)
	case "jsondict":
		errmsg = v.checkJsondict(data, field, fieldCheck)
	default:
		panic(fmt.Sprintf("Invalid field type '%s' for ginstarter.DataValidator. Please check your code!", field))
	}

	return errmsg
}

func (v *DataValidator) handleError(prop string, field string, fieldCheck map[string]interface{}, additionalMsg ...string) error {
	errProp := prop + "_errmsg"
	if errmsg, ok := fieldCheck[errProp]; ok {
		return fmt.Errorf("%v", errmsg)
	}

	// 默认错误消息
	errmsg := fmt.Sprintf("Illegal value for field `%s`.", field)
	if prop == "required" {
		errmsg = fmt.Sprintf("field `%s` must be provided", field)
	}
	if len(additionalMsg) != 0 {
		errmsg += " " + strings.Join(additionalMsg, " ")
	}
	return errors.New(errmsg)
}

func (v *DataValidator) checkAutoConvert(fType string, value interface{}, field string, fieldCheck map[string]interface{}) (interface{}, error) {
	// 允许auto_convert，则做类型转换。
	if autoConvert, _ := fieldCheck["auto_convert"].(bool); autoConvert {
		strVal, ok := value.(string)
		if !ok {
			msg := fmt.Sprintf("'auto_convert' requires the value must be a string, for field `%s`", field)
			return nil, v.handleError("auto_convert", field, fieldCheck, msg)
		} else {
			convertedVal, err := convertStrVal(fType, strVal)
			if err != nil {
				msg := fmt.Sprintf("Failed to convert value '%s' to int for field `%s`. %s", strVal, field, err.Error())
				return nil, v.handleError("auto_convert", field, fieldCheck, msg)
			}
			return convertedVal, nil
		}
	}

	// 不允许auto_convert，则返回校验失败。
	return nil, v.handleError("type", field, fieldCheck, fmt.Sprintf("Value must be %s type.", fType))
}

func checkChocies(value interface{}, choices []interface{}) bool {
	var valKey interface{} = "value"
	for _, item := range choices {
		if realItem, ok := item.(map[interface{}]interface{}); ok {
			if val, ok := realItem[valKey]; ok && val == value {
				return true
			}
		}
		if realItem, ok := item.(map[string]interface{}); ok {
			if val, ok := realItem["value"]; ok && val == value {
				return true
			}
		}
		if value == item {
			return true
		}
	}
	return false
}

func (v *DataValidator) checkInt(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	var expectVal int
	if expectVal, ok = val.(int); !ok {
		if val64, ok := val.(int64); ok {
			expectVal = int(val64)
		} else if val32, ok := val.(int32); ok {
			expectVal = int(val32)
		} else if jsonNum, ok := val.(json.Number); ok {
			tmp64, err := jsonNum.Int64()
			if err != nil {
				return fmt.Errorf("failed to convert json.Number to int64. %s", err.Error())
			}
			expectVal = int(tmp64)
		} else {
			iVal, err := v.checkAutoConvert("int", val, field, fieldCheck)
			if err != nil {
				return err
			}
			expectVal = iVal.(int)
		}
		data[field] = expectVal
	}

	// 默认允许零值
	not_empty, _ := fieldCheck["not_empty"].(bool)
	if !not_empty && expectVal == 0 {
		return nil
	}

	// choice
	if choices, ok := fieldCheck["choices"].([]interface{}); ok {
		val = data[field]
		if !checkChocies(val, choices) {
			return v.handleError("choices", field, fieldCheck, fmt.Sprintf("value not in choices constraints, for field '%s'", field))
		}
		return nil
	}

	// 最小值校验
	if min, ok := fieldCheck["min"].(int); ok {
		if expectVal < min {
			msg := fmt.Sprintf("Value less than min constraints, for field '%s'.", field)
			return v.handleError("min", field, fieldCheck, msg)
		}
	}

	// 最大值校验
	if max, ok := fieldCheck["max"].(int); ok {
		if expectVal > max {
			msg := fmt.Sprintf("Value greater than max constraints, for field '%s'.", field)
			return v.handleError("max", field, fieldCheck, msg)
		}
	}

	return nil
}

func (v *DataValidator) checkFloat(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.(float64)
	if !ok {
		if jsonNum, ok := val.(json.Number); ok {
			tmpVal, err := jsonNum.Float64()
			if err != nil {
				return fmt.Errorf("failed to convert json.Number to float64. %s", err.Error())
			}
			expectVal = tmpVal
		} else {
			iVal, err := v.checkAutoConvert("float", val, field, fieldCheck)
			if err != nil {
				return err
			}
			expectVal = iVal.(float64)
		}
		data[field] = expectVal
	}

	// 默认允许零值
	not_empty, _ := fieldCheck["not_empty"].(bool)
	if !not_empty && expectVal == 0 {
		return nil
	}

	// choices
	if choices, ok := fieldCheck["choices"].([]interface{}); ok {
		val = data[field]
		if !checkChocies(val, choices) {
			return v.handleError("choices", field, fieldCheck, fmt.Sprintf("value not in choices constraints, for field '%s'", field))
		}
		return nil
	}

	// 最小值校验
	if min, ok := fieldCheck["min"].(float64); ok {
		if expectVal < min {
			msg := fmt.Sprintf("Value less than min constraints, for field '%s'.", field)
			return v.handleError("min", field, fieldCheck, msg)
		}
	}
	// 最大值校验
	if max, ok := fieldCheck["max"].(float64); ok {
		if expectVal > max {
			msg := fmt.Sprintf("Value greater than max constraints, for field '%s'.", field)
			return v.handleError("max", field, fieldCheck, msg)
		}
	}

	return nil
}

func (v *DataValidator) checkBool(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	var expectVal bool

	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	_, ok = val.(bool)
	if !ok {
		iVal, err := v.checkAutoConvert("bool", val, field, fieldCheck)
		if err != nil {
			return err
		}
		expectVal = iVal.(bool)
		data[field] = expectVal
	}

	return nil
}

func (v *DataValidator) checkString(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.(string)
	if !ok {
		return v.handleError("type", field, fieldCheck, fmt.Sprintf("value not a string, for field '%s'", field))
	}

	// 默认允许零值
	not_empty, _ := fieldCheck["not_empty"].(bool)
	if !not_empty && expectVal == "" {
		return nil
	}

	// trim_space
	if ok, _ := fieldCheck["trim_space"].(bool); ok {
		expectVal = strings.TrimSpace(expectVal)
		data[field] = expectVal
	}

	// choices
	if choices, ok := fieldCheck["choices"].([]interface{}); ok {
		val = data[field]
		if !checkChocies(val, choices) {
			return v.handleError("choices", field, fieldCheck, fmt.Sprintf("value not in choices constraints, for field '%s'", field))
		}
		return nil
	}

	// not_empty
	if not_empty, ok := fieldCheck["not_empty"].(bool); ok {
		if not_empty {
			cleanVal := strings.TrimSpace(expectVal)
			if cleanVal == "" {
				return v.handleError("not_empty", field, fieldCheck, fmt.Sprintf("value cannot be empty, for field '%s'", field))
			}
		}
	}

	// 正则校验
	if ireStr, ok := fieldCheck["regexp"]; ok {
		reStr, ok := ireStr.(string)
		if !ok {
			slog.Warning("Invalid data validator constraints. Prop 'regexp' must be a string.")
		} else {
			valBytes := []byte(expectVal)
			matched, _ := regexp.Match(reStr, valBytes)
			if !matched {
				return v.handleError("regexp", field, fieldCheck, fmt.Sprintf("Not matched with regexp constraint, for field '%s'", field))
			}
		}
	}

	// 最小长度校验
	if minlen, ok := fieldCheck["minlen"].(int); ok {
		if len(expectVal) < minlen {
			return v.handleError("minlen", field, fieldCheck, fmt.Sprintf("Value less than minlen constraint, for field '%s'", field))
		}
	}

	// 最大长度校验
	if maxlen, ok := fieldCheck["maxlen"].(int); ok {
		if len(expectVal) > maxlen {
			return v.handleError("maxlen", field, fieldCheck, fmt.Sprintf("Value less than maxlen constraint, for field '%s'", field))
		}
	}

	return nil
}

func (v *DataValidator) checkIpaddr(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.(string)
	if !ok {
		if val == nil {
			expectVal = ""
		} else {
			return v.handleError("type", field, fieldCheck, fmt.Sprintf("'ipaddr' value mast be a string, for field '%s'", field))
		}
	}
	expectVal = strings.TrimSpace(expectVal)
	data[field] = expectVal

	// 空值验证
	if expectVal == "" {
		if not_empty, ok := fieldCheck["not_empty"].(bool); ok {
			if not_empty {
				return v.handleError("not_empty", field, fieldCheck, fmt.Sprintf("ipaddress value cannot be empty, for field '%s'", field))
			}
		}
		return nil
	}

	// 校验IP
	if address := net.ParseIP(expectVal); address == nil {
		return v.handleError("type", field, fieldCheck, fmt.Sprintf("value not a ipaddr, for field '%s'", field))
	}

	// choices
	if choices, ok := fieldCheck["choices"].([]interface{}); ok {
		val = data[field]
		if !checkChocies(val, choices) {
			return v.handleError("choices", field, fieldCheck, fmt.Sprintf("value not in choices constraints, for field '%s'", field))
		}
		return nil
	}

	return nil
}

func (v *DataValidator) checkDatetime(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.(string)
	if !ok {
		if val == nil {
			expectVal = ""
		} else {
			return v.handleError("type", field, fieldCheck, fmt.Sprintf("'dateime' value must be a string, for field '%s'", field))
		}
	}

	expectVal = strings.TrimSpace(expectVal)
	data[field] = expectVal

	// 空值验证
	if expectVal == "" {
		if not_empty, ok := fieldCheck["not_empty"].(bool); ok {
			if not_empty {
				return v.handleError("not_empty", field, fieldCheck, fmt.Sprintf("datetime value cannot be empty, for field '%s'", field))
			}
		}
		return nil
	}

	// 校验时间字符串
	layout := "2006-01-02 15:04:05.999999"
	if _, err := time.ParseInLocation(layout, expectVal, time.Local); err != nil {
		return v.handleError("type", field, fieldCheck, fmt.Sprintf("value not in format 'yyyy-mm-dd hh:mm:ss', for field '%s'", field))
	}

	// choices
	if choices, ok := fieldCheck["choices"].([]interface{}); ok {
		val = data[field]
		if !checkChocies(val, choices) {
			return v.handleError("choices", field, fieldCheck, fmt.Sprintf("value not in choices constraints, for field '%s'", field))
		}
		return nil
	}

	return nil
}

func (v *DataValidator) checkTimestamp(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	_, ok = val.(int64)
	if !ok {
		if valJson, ok := val.(json.Number); !ok {
			expVal, err := valJson.Int64()
			if err != nil {
				return fmt.Errorf("failed to convert json.Number to int64 timestamp. %s", err.Error())
			}
			data[field] = expVal
		} else {
			iVal, err := v.checkAutoConvert("timestamp", val, field, fieldCheck)
			if err != nil {
				return err
			}
			data[field] = iVal.(int64)
		}
	}

	// 默认允许零值
	expectVal := data[field].(int64)
	not_empty, _ := fieldCheck["not_empty"].(bool)
	if !not_empty && expectVal == 0 {
		return nil
	}

	// choices
	if choices, ok := fieldCheck["choices"].([]interface{}); ok {
		val = data[field]
		if !checkChocies(val, choices) {
			return v.handleError("choices", field, fieldCheck, fmt.Sprintf("value not in choices constraints, for field '%s'", field))
		}
		return nil
	}

	return nil
}

// 用于list类型生成子元素的fieldCheck规则。（去掉item前缀）
func makeSubCheck(fieldCheck map[string]interface{}, flag string) map[string]interface{} {
	var pre string
	switch flag {
	case "ForListItem":
		pre = "item_"
	case "ForDictKey":
		pre = "key_"
	case "ForDictVal":
		pre = "value_"
	default:
		panic("Inner func usage error: ginstarter.makeSubCheck()")
	}

	subCheck := map[string]interface{}{}

	for checkName, checkVal := range fieldCheck {
		if strings.HasPrefix(checkName, pre) {
			subName := strings.TrimPrefix(checkName, pre)
			subCheck[subName] = checkVal
		}
	}
	return subCheck
}

func (v *DataValidator) checkList(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.([]interface{})
	if !ok {
		return v.handleError("list", field, fieldCheck, fmt.Sprintf("val not a 'list', for field '%s'", field))
	}
	itemTypeI, ok := fieldCheck["item_type"]
	if !ok {
		return nil
	}
	itemType, ok := itemTypeI.(string)
	if !ok {
		panic(fmt.Sprintf("Invalid validator for list field '%s'. Prop 'item_type' must be a string!", field))
	}

	// 最小长度校验
	if minlen, ok := fieldCheck["minlen"].(int); ok {
		if len(expectVal) < minlen {
			return v.handleError("minlen", field, fieldCheck, fmt.Sprintf("Value less than minlen constraint, for field '%s'", field))
		}
	}

	// 最大长度校验
	if maxlen, ok := fieldCheck["maxlen"].(int); ok {
		if len(expectVal) > maxlen {
			return v.handleError("maxlen", field, fieldCheck, fmt.Sprintf("Value less than maxlen constraint, for field '%s'", field))
		}
	}

	// 对元素作递归校验
	for _, itemVal := range expectVal {
		itemData := map[string]interface{}{
			"list_item_value": itemVal,
		}
		itemCheck := makeSubCheck(fieldCheck, "ForListItem")
		err := v.typeCheckingDispatch(itemData, "list_item_value", itemType, itemCheck)
		if err != nil {
			return err
		}
	}

	return nil
}

func (v *DataValidator) checkDict(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.(map[string]interface{})
	if !ok {
		expectValI, ok := val.(map[interface{}]interface{})
		if !ok {
			return v.handleError("dict", field, fieldCheck, fmt.Sprintf("val not a 'dict', for field '%s'", field))
		}
		expectVal = map[string]interface{}{}
		for keyI, valueI := range expectValI {
			key, ok := keyI.(string)
			if !ok {
				return v.handleError("dict", field, fieldCheck, fmt.Sprintf("key for 'dict' must be a string, for field '%s'", field))
			}
			expectVal[key] = valueI
		}
	}
	slog.Debug(fmt.Sprintf("{ginstarter.checkDict()} expectVal: %v", expectVal))

	// format约束会忽略其他约束。format将触发递归校验。
	if formatI, ok := fieldCheck["dict_format"].(map[interface{}]interface{}); ok {
		format := map[string]map[string]interface{}{}
		for keyI, valI := range formatI {
			key, ok := keyI.(string)
			if !ok {
				// validator中format定义错误，key必须是个string
				panic(fmt.Sprintf("Invalid validator for field '%s'. Key in dict_format must be string. Please check your validator code.", field))
			}
			val, ok := valI.(map[interface{}]interface{})
			if !ok {
				// validator中format定义错误，val必须是个map
				panic(fmt.Sprintf("Invalid validator for field '%s'. Val in dict_format must be map. Please check your validator code.", field))
			}
			valCheck := map[string]interface{}{}
			for checkNameI, checkValI := range val {
				checkName, ok := checkNameI.(string)
				if !ok {
					// validator中format定义错误，val必须是个map[string]interface{}
					panic(fmt.Sprintf("Invalid validator for field '%s'. Val in dict_format must be map[string]interace{}. Please check your validator code.", field))
				}
				valCheck[checkName] = checkValI
			}
			format[key] = valCheck
		}
		subValidator := DataValidator{Validator: format, FixedFields: true}
		return subValidator.DataValidate(expectVal)
	}

	// 最小长度校验
	if minlen, ok := fieldCheck["minlen"].(int); ok {
		if len(expectVal) < minlen {
			return v.handleError("minlen", field, fieldCheck, fmt.Sprintf("Value less than minlen constraint, for field '%s'", field))
		}
	}

	// 最大长度校验
	if maxlen, ok := fieldCheck["maxlen"].(int); ok {
		if len(expectVal) > maxlen {
			return v.handleError("maxlen", field, fieldCheck, fmt.Sprintf("Value less than maxlen constraint, for field '%s'", field))
		}
	}

	// key约束，直接复用checkString。同时生成递归校验的子校验器
	keyCheck := makeSubCheck(fieldCheck, "ForDictKey")
	valCheck := makeSubCheck(fieldCheck, "ForDictVal")
	subFieldCheck := map[string]map[string]interface{}{}
	for key := range expectVal {
		if len(keyCheck) > 0 {
			keyData := map[string]interface{}{"dict_key": key}
			if err := v.checkString(keyData, "dict_key", keyCheck); err != nil {
				return err
			}
		}
		subFieldCheck[key] = valCheck
	}

	// val约束。利用子校验器作递归校验。
	if len(valCheck) > 0 {
		subValidator := DataValidator{Validator: subFieldCheck}
		return subValidator.DataValidate(expectVal)
	}

	return nil
}

func (v *DataValidator) checkJsondict(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.(string)
	if !ok {
		return v.handleError("type", field, fieldCheck, fmt.Sprintf("'jsondict' value must be a string, for field '%s'", field))
	}
	expectVal = strings.TrimSpace(expectVal)
	data[field] = expectVal

	// 校验jsondict是否合法
	realData := make(map[string]interface{})
	if err := json.Unmarshal([]byte(expectVal), &realData); err != nil {
		return v.handleError("type", field, fieldCheck, fmt.Sprintf("Value not a valid 'jsondict', for field '%s'", field))
	}

	// 对内容的校验，复用dict校验逻辑
	fieldData := map[string]interface{}{field: realData}
	return v.checkDict(fieldData, field, fieldCheck)
}

func (v *DataValidator) checkJsonlist(data map[string]interface{}, field string, fieldCheck map[string]interface{}) error {
	val, ok := data[field]
	if !ok {
		return nil
	}

	// 类型校验
	expectVal, ok := val.(string)
	if !ok {
		return v.handleError("type", field, fieldCheck, fmt.Sprintf("'jsonlist' value must be a string, for field '%s'", field))
	}
	expectVal = strings.TrimSpace(expectVal)
	data[field] = expectVal

	// 校验jsonlist是否合法
	realData := []interface{}{}
	if err := json.Unmarshal([]byte(expectVal), &realData); err != nil {
		return v.handleError("type", field, fieldCheck, fmt.Sprintf("Value not a valid 'jsonlist', for field '%s'", field))
	}

	// 对内容的校验，复用list校验逻辑
	fieldData := map[string]interface{}{field: realData}
	return v.checkList(fieldData, field, fieldCheck)
}

// 若要对Form结构体进行数据校验，请先将结构体转为map[string]interface{}.
// 不要使用json序列化工具作转换，这会将int、float类型的全部转化为float64。
// 推荐使用工具：github.com/fatih/structs。或者手动转换。
func (v *DataValidator) DataValidate(data map[string]interface{}) error {
	// 若无数据校验要求，直接返回成功。
	if len(v.Validator) == 0 {
		return nil
	}

	for field, fieldCheck := range v.Validator {
		// 先检查required属性。
		if required, ok := v.Validator[field]["required"].(bool); ok {
			if required {
				if _, ok := data[field]; !ok {
					return v.handleError("required", field, fieldCheck)
				}
			}
		}

		// 检查是否设定了type
		fType, ok := v.Validator[field]["type"].(string)
		if !ok {
			panic(fmt.Sprintf("Invalid validator for field '%s'. Prop 'type' is required. Please Check your Validator code.", field))
		}

		// 分类型校验数据
		errmsg := v.typeCheckingDispatch(data, field, fType, fieldCheck)
		if errmsg != nil {
			return errmsg
		}
	}

	if v.FixedFields {
		for field := range data {
			if _, ok := v.Validator[field]; !ok {
				delete(data, field)
			}
		}
	}
	return nil
}

func GetFieldTypeOptions() []string {
	return []string{
		"int",
		"float",
		"string",
		"bool",
		"ipaddr",
		"datetime",
		"timestamp",
		"list",
		"dict",
		"jsonlist",
		"jsondict",
	}
}
