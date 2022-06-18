package nebula_client

import (
	slog "codeops.didachuxing.com/lordaeron/go-toolbox/simplelog"
	"codeops.didachuxing.com/lordaeron/go-toolbox/tools"

	"errors"
	"fmt"
	"strings"
)

// 生成INSERT VERTEX语句
func getInsertVertexNGQL(tag string, schema map[string]string, data map[string]interface{}) (string, error) {
	// 检查提供的字段合不合法
	for field := range data {
		if field == "vid" {
			continue
		}
		if _, ok := schema[field]; !ok {
			return "", fmt.Errorf("illegal field '%s' for vertex insert on tag '%s'", field, tag)
		}
	}

	vid := data["vid"].(string)
	ngqlPrefix := "INSERT VERTEX"
	tagNGQL := fmt.Sprintf("%s(", tag)
	vertexNGQL := fmt.Sprintf("\"%s\":(", vid)
	for field, fieldDef := range schema {
		// 如果没有提供这个字段的值，则忽略此字段
		fieldVal, ok := data[field]
		if !ok {
			continue
		}
		fieldValNGQL, err := getNGQLValue(fieldDef, fieldVal)
		if err != nil {
			return "", err
		}
		tagNGQL = fmt.Sprintf("%s%s, ", tagNGQL, field)
		vertexNGQL = fmt.Sprintf("%s%s, ", vertexNGQL, fieldValNGQL)
	}
	tagNGQL = strings.TrimSuffix(tagNGQL, ", ") + ")"
	vertexNGQL = strings.TrimSuffix(vertexNGQL, ", ") + ")"

	ngql := fmt.Sprintf("%s %s VALUES %s;", ngqlPrefix, tagNGQL, vertexNGQL)
	return ngql, nil
}

// 检查vid是否存在与某个tag上. 返回err == nil 表示存在，否责表示不存在，或者出错。
func HasVertexExistOnTag(tag string, vid string) (bool, error) {
	ngql := fmt.Sprintf("FETCH PROP ON %s \"%s\"", tag, vid)
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return false, err
	}
	if res.GetRowSize() <= 0 {
		return false, nil
	}
	return true, nil
}

// 基于Tag DataSchema插入一个新的vertex。
func InsertVertex(tds DataSchema, flags ...string) error {
	// 校验数据
	tag, schema := tds.GetSchema()
	data := tds.GetDataInSchema()
	if err := ValidateVertex(tag, schema, data); err != nil {
		return err
	}
	vid := data["vid"].(string)

	// 检查数据是否存在，存在则抛错（nebula默认存在的话，会直接覆盖，这不是我们期望的。）
	if !tools.IsStrInSlice("AllowReplace", flags) {
		isExists, err := HasVertexExistOnTag(tag, vid)
		if err != nil {
			return err
		}
		if isExists {
			return fmt.Errorf("vid '%s' already exist", vid)
		}
	}

	//构建ngql语句
	ngql, err := getInsertVertexNGQL(tag, schema, data)
	if err != nil {
		return err
	}
	slog.Debug(fmt.Sprintf("{nebula_client.InsertVertex()} ngql: %s", ngql))

	// 连接DB，执行ngql语句
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to Insert Vertex '%s'. %s.", vid, err.Error())
		return errors.New(msg)
	}
	return nil
}

// 基于vid删除vertex。
func DeleteVertex(tds DataSchema) error {
	// 校验数据
	tag, schema := tds.GetSchema()
	data := tds.GetDataInSchema()
	if err := ValidateVertex(tag, schema, data); err != nil {
		return err
	}

	// 构造ngql语句
	vid := data["vid"].(string)
	if vid == "" {
		return errors.New("vid should not be empty")
	}
	ngql := fmt.Sprintf("DELETE VERTEX \"%s\";", vid)
	slog.Debug(fmt.Sprintf("{nebula_client.DeleteVertex()} ngql: %s", ngql))

	// 连接DB，执行ngql语句
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to  delete Vertex '%s'. %s.", vid, err.Error())
		return errors.New(msg)
	}
	return nil
}

// 基于tds更新一个Vertex对应tag的所有属性，需要将所有属性数据设置在tds的结构体字段中。
// `skipFields`用于指定schema中的哪些字段不作修改。一般可用于有只读字段数据的全局替换。
func ReplaceVertex(tds DataSchema, skipFields ...string) error {
	// 校验数据
	tag, schema := tds.GetSchema()
	data := tds.GetDataInSchema()
	if err := ValidateVertex(tag, schema, data); err != nil {
		return err
	}

	// 检查提供的字段合不合法
	for field := range data {
		if field == "vid" {
			continue
		}
		if _, ok := schema[field]; !ok {
			return fmt.Errorf("illegal field '%s' for vertex update on tag '%s'", field, tag)
		}
	}

	// 构造ngql语句
	vid := data["vid"].(string)
	ngql := fmt.Sprintf("UPDATE VERTEX ON %s \"%s\" SET ", tag, vid)
	for field, fieldDef := range schema {
		if tools.IsStrInSlice(field, skipFields) {
			continue
		}
		if _, ok := data[field]; !ok {
			continue
		}
		fieldVal, err := getNGQLValue(fieldDef, data[field])
		if err != nil {
			return err
		}
		ngql += fmt.Sprintf("%s = %s, ", field, fieldVal)
	}
	ngql = strings.TrimSuffix(ngql, ", ") + ";"
	slog.Debug(fmt.Sprintf("{nebula_client.ReplaceVertex()} ngql: %s", ngql))

	// 连接DB，执行ngql语句
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to Replace Vertex '%s'. %s.", vid, err.Error())
		return errors.New(msg)
	}
	return nil
}

// 基于tds局部更新一个vertex的tag属性，需在参数`updateFields`中指定需要更新的属性字段
func UpdateVertex(tds DataSchema, updateFields []string) error {
	// 校验数据
	tag, schema := tds.GetSchema()
	data := tds.GetDataInSchema()
	if err := ValidateVertex(tag, schema, data); err != nil {
		return err
	}
	if len(updateFields) == 0 {
		return errors.New("no field specified in param `updateFields`")
	}

	// 构造nGQL
	vid := data["vid"].(string)
	ngql := fmt.Sprintf("UPDATE VERTEX ON %s \"%s\" SET ", tag, vid)
	for _, field := range updateFields {
		fieldDef, ok := schema[field]
		if !ok {
			return fmt.Errorf("illegal field '%s' found in param `updateFields`", field)
		}
		if _, ok := data[field]; !ok {
			return fmt.Errorf("value for field '%s' not provided", field)
		}
		fieldVal, err := getNGQLValue(fieldDef, data[field])
		if err != nil {
			return err
		}
		ngql += fmt.Sprintf("%s = %s, ", field, fieldVal)
	}
	ngql = strings.TrimSuffix(ngql, ", ") + ";"
	slog.Debug(fmt.Sprintf("{nebula_client.UpdateVertex()} ngql: %s", ngql))

	// 执行nGQL
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to Update Vertex '%s'. %s.", vid, err.Error())
		return errors.New(msg)
	}
	return nil
}

// 注意，参数fields的字段顺序，应和查询是ngql的yield property(vertex)...语句顺序保持一致。
// 且fields的第一个元素必须是"VertexID"(for lookup语句) 或者 "vid" (for go语句)
func ParseStringTable(table [][]string, fields []string, schema map[string]string) ([]map[string]interface{}, error) {
	resData := []map[string]interface{}{}

	if len(table) <= 1 {
		return resData, nil
	}
	// if fields[0] != "VertexID" && fields[0] != "vid" {
	// 	return resData, errors.New("first item in param `fields` must be 'VertexID' or 'vid'")
	// }

	table_title := table[0]
	for _, row := range table[1:] {
		rowData := map[string]interface{}{}
		for i, field := range fields {
			fieldDef := schema[field]
			if table_title[i] == "vid" || table_title[i] == "VertexID" || table_title[i] == "edgeType" {
				fieldDef = "string"
			}
			fieldVal, err := getRealValue(fieldDef, row[i])
			if err != nil {
				return nil, err
			}

			// 转换nebula内置字段：VertexID -> vid, SrcVID -> src_vid, DstVID -> dst_vid
			if i == 0 && field == "VertexID" {
				rowData["vid"] = fieldVal
			} else if i == 0 && field == "SrcVID" {
				rowData["src_vid"] = fieldVal
			} else if i == 1 && field == "DstVID" {
				rowData["dst_vid"] = fieldVal
			} else {
				rowData[field] = fieldVal
			}
		}
		resData = append(resData, rowData)
	}

	return resData, nil
}

// 必须在tds里提供vid, 然后此函数会到数据库查询tds的其他属性, 以dataInSchema的形式返回数据（当前版本不对tds的struct字段做数据填充）。
func FetchVertexData(tds DataSchema) (map[string]interface{}, error) {
	// 校验vid
	data := tds.GetDataInSchema()
	vid, _ := data["vid"].(string)
	if err := ValidateName(vid); err != nil {
		return nil, err
	}

	// 构造ngql
	tag, schema := tds.GetSchema()
	ngql := fmt.Sprintf("FETCH PROP ON %s \"%s\" YIELD ", tag, vid)
	fields := []string{"VertexID"}
	for field := range schema {
		ngql += fmt.Sprintf("properties(vertex).%s as %s, ", field, field)
		fields = append(fields, field)
	}
	ngql = strings.TrimSuffix(ngql, ", ") + ";"
	slog.Debug(fmt.Sprintf("{nebula_client.FetchVertexData()} ngql: %s", ngql))

	// 查询数据
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	// 解析数据到result
	dataTable := res.AsStringTable()
	resData, err := ParseStringTable(dataTable, fields, schema)
	if err != nil {
		return nil, err
	}
	if len(resData) == 0 {
		return nil, fmt.Errorf("no data found with vid '%s'", vid)
	}
	result := resData[0]
	return result, nil
}

// 通过tag查vertex属性数据；可以通过field做精确过滤。
// 默认按created_at字段顺序排列。
func LookupVertexes(tds DataSchema, filter map[string]interface{}, showFields ...string) ([]map[string]interface{}, error) {
	// 构造Lookup ngql
	tag, schema := tds.GetSchema()
	ngql := fmt.Sprintf("LOOKUP ON %s ", tag)
	if len(filter) != 0 {
		ngql += "WHERE "
		for field, val := range filter {
			fieldDef, ok := schema[field]
			if !ok {
				return nil, fmt.Errorf("filter with illegal field '%s'", field)
			}
			valStr, err := getNGQLValue(fieldDef, val)
			if err != nil {
				return nil, err
			}
			ngql += fmt.Sprintf("%s.%s == %s AND ", tag, field, valStr)
		}
		ngql = strings.TrimSuffix(ngql, "AND ") + " "
	}
	fields := []string{"VertexID"}
	needOrder := false
	if len(showFields) == 0 {
		ngql += "YIELD "
		needOrder = true
		for field := range schema {
			ngql += fmt.Sprintf("properties(vertex).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	} else if len(showFields) == 1 && showFields[0] == "vid" {
		ngql = strings.TrimSuffix(ngql, " ")
	} else {
		ngql += "YIELD "
		needOrder = true
		if _, ok := schema["created_at"]; ok {
			showFields = append(showFields, "created_at")
		}
		for _, field := range showFields {
			if field == "vid" {
				continue
			}
			if _, ok := schema[field]; !ok {
				return nil, fmt.Errorf("invalid show field '%s'", field)
			}
			ngql += fmt.Sprintf("properties(vertex).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	}
	ngql = strings.TrimSuffix(ngql, ", ")
	if _, ok := schema["created_at"]; ok && needOrder {
		ngql += " | ORDER BY $-.created_at ASC"
	}
	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.LookupVertexes()} ngql: %s", ngql))

	// 查询数据
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	// 解析数据到result
	dataTable := res.AsStringTable()
	result, err := ParseStringTable(dataTable, fields, schema)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// 分页查询、搜过、过滤
type Queryer struct {
	PageSize          int      `form:"page_size"` // 0 表示不做分页
	PageIndex         int      `form:"page_index"`
	SearchFieldsAlias []string `form:"search_fields[]"` // 兼容前端axios.js
	SearchFields      []string `form:"search_fields"`   // 空值表示不做搜索过滤
	SearchValue       string   `form:"search_value"`
	Explicitly        bool     `form:"explicitly"` // true 表示采用'=='匹配，否则采用'starts with'匹配（目前neubla lookup模糊匹配只支持'starts with'）
}

func (q *Queryer) LookupVertexesWithPagination(tds DataSchema, showFields ...string) (int, []map[string]interface{}, error) {
	// 构造Lookup ngql
	tag, schema := tds.GetSchema()
	ngql := fmt.Sprintf("LOOKUP ON %s ", tag)

	// 拼接where语句
	if len(q.SearchFieldsAlias) > 0 {
		q.SearchFields = q.SearchFieldsAlias
	}
	if len(q.SearchFields) > 0 {
		ngql += "WHERE "
		for _, field := range q.SearchFields {
			fieldDef, ok := schema[field]
			if !ok {
				return 0, nil, fmt.Errorf("param 'search_fields' got illegal field '%s'", field)
			}
			fieldtype := strings.ToUpper(strings.Split(strings.TrimSpace(fieldDef), " ")[0])
			filterableTypes := NumberTypes()
			searchableTypes := SearchableTypes()
			if !tools.IsStrInSlice(fieldtype, filterableTypes) && !tools.IsStrInSlice(fieldtype, searchableTypes) {
				continue
			}
			valStr, err := getNGQLValue(fieldDef, q.SearchValue)
			if err != nil {
				return 0, nil, err
			}
			if q.Explicitly || tools.IsStrInSlice(fieldtype, NumberTypes()) {
				ngql += fmt.Sprintf("%s.%s == %s OR ", tag, field, valStr)
			} else {
				ngql += fmt.Sprintf("%s.%s STARTS WITH %s OR ", tag, field, valStr)
			}
		}
		ngql = strings.TrimSuffix(ngql, "WHERE ")
		ngql = strings.TrimSuffix(ngql, "OR ") + " "
	}

	// 拼接yield语句
	fields := []string{"VertexID"}
	needOrder := false
	if len(showFields) == 0 {
		ngql += "YIELD "
		needOrder = true
		for field := range schema {
			ngql += fmt.Sprintf("properties(vertex).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	} else if len(showFields) == 1 && showFields[0] == "vid" {
		ngql = strings.TrimSuffix(ngql, " ")
	} else {
		ngql += "YIELD "
		needOrder = true
		if _, ok := schema["created_at"]; ok {
			showFields = append(showFields, "created_at")
		}
		for _, field := range showFields {
			if field == "vid" {
				continue
			}
			if _, ok := schema[field]; !ok {
				return 0, nil, fmt.Errorf("invalid show field '%s'", field)
			}
			ngql += fmt.Sprintf("properties(vertex).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	}
	ngql = strings.TrimSuffix(ngql, ", ")

	// 查询总数
	totalGL := ngql + ";"
	slog.Debug(fmt.Sprintf("{nebula_client.LookupVertexes()} Total ngql: %s", ngql))
	totalRes, err := ConnectAndExcute(totalGL)
	if err != nil {
		return 0, nil, err
	}
	total := totalRes.GetRowSize()

	// 排序
	if _, ok := schema["created_at"]; ok && needOrder {
		ngql += " | ORDER BY $-.created_at ASC"
	}

	// 最后，分页
	if q.PageSize > 0 {
		index := 1
		if q.PageIndex > 1 {
			index = q.PageIndex
		}
		offset := (index - 1) * q.PageSize
		ngql += fmt.Sprintf(" | LIMIT %d, %d", offset, q.PageSize)
	}

	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.LookupVertexes()} ngql: %s", ngql))

	// 查询数据
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return 0, nil, err
	}

	// 解析数据到result
	dataTable := res.AsStringTable()
	result, err := ParseStringTable(dataTable, fields, schema)
	if err != nil {
		return 0, nil, err
	}

	return total, result, nil
}

// Queryer的升级版，支持多value并联查询。
type Queryer2 struct {
	PageSize          int      `form:"page_size"` // 0 表示不做分页
	PageIndex         int      `form:"page_index"`
	SearchFieldsAlias []string `form:"search_fields[]"` // 兼容前端axios.js
	SearchFields      []string `form:"search_fields"`   // 空值表示不做搜索过滤
	SearchValuesAlias []string `form:"search_values[]"` // 兼容前端axios.js
	SearchValues      []string `form:"search_values"`
	Explicitly        bool     `form:"explicitly"` // true 表示采用'=='匹配，否则采用'starts with'匹配（目前neubla lookup模糊匹配只支持'starts with'）
}

func (q *Queryer2) LookupVertexesWithPagination(tds DataSchema, showFields ...string) (int, []map[string]interface{}, error) {
	// 构造Lookup ngql
	tag, schema := tds.GetSchema()
	ngql := fmt.Sprintf("LOOKUP ON %s ", tag)

	// 拼接where语句
	if len(q.SearchFieldsAlias) > 0 {
		q.SearchFields = q.SearchFieldsAlias
	}
	if len(q.SearchValuesAlias) > 0 {
		q.SearchValues = q.SearchValuesAlias
	}
	if len(q.SearchFields) > 0 {
		ngql += "WHERE "
		for _, field := range q.SearchFields {
			fieldDef, ok := schema[field]
			if !ok {
				return 0, nil, fmt.Errorf("param 'search_fields' got illegal field '%s'", field)
			}
			fieldtype := strings.ToUpper(strings.Split(strings.TrimSpace(fieldDef), " ")[0])
			filterableTypes := NumberTypes()
			searchableTypes := SearchableTypes()
			if !tools.IsStrInSlice(fieldtype, filterableTypes) && !tools.IsStrInSlice(fieldtype, searchableTypes) {
				continue
			}

			if len(q.SearchValues) == 0 {
				valStr, err := getNGQLValue(fieldDef, "")
				if err != nil {
					return 0, nil, err
				}
				if q.Explicitly || tools.IsStrInSlice(fieldtype, NumberTypes()) {
					ngql += fmt.Sprintf("%s.%s == %s OR ", tag, field, valStr)
				} else {
					ngql += fmt.Sprintf("%s.%s STARTS WITH %s OR ", tag, field, valStr)
				}
			} else if len(q.SearchValues) == 1 {
				valStr, err := getNGQLValue(fieldDef, q.SearchValues[0])
				if err != nil {
					return 0, nil, err
				}
				if q.Explicitly || tools.IsStrInSlice(fieldtype, NumberTypes()) {
					ngql += fmt.Sprintf("%s.%s == %s OR ", tag, field, valStr)
				} else {
					ngql += fmt.Sprintf("%s.%s STARTS WITH %s OR ", tag, field, valStr)
				}
			} else {
				valStrs := []string{}
				for _, val := range q.SearchValues {
					valStr, err := getNGQLValue(fieldDef, val)
					if err != nil {
						return 0, nil, err
					}
					valStrs = append(valStrs, valStr)
				}
				sub := "("
				for _, val := range valStrs {
					if q.Explicitly || tools.IsStrInSlice(fieldtype, NumberTypes()) {
						sub += fmt.Sprintf("%s.%s == %s OR ", tag, field, val)
					} else {
						sub += fmt.Sprintf("%s.%s STARTS WITH %s OR ", tag, field, val)
					}
				}
				sub = strings.TrimSuffix(sub, "OR ") + ")"
				ngql += sub + " OR "
			}
		}
		ngql = strings.TrimSuffix(ngql, "WHERE ")
		ngql = strings.TrimSuffix(ngql, "OR ") + " "
	}

	// 拼接yield语句
	fields := []string{"VertexID"}
	needOrder := false
	if len(showFields) == 0 {
		ngql += "YIELD "
		needOrder = true
		for field := range schema {
			ngql += fmt.Sprintf("properties(vertex).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	} else if len(showFields) == 1 && showFields[0] == "vid" {
		ngql = strings.TrimSuffix(ngql, " ")
	} else {
		ngql += "YIELD "
		needOrder = true
		if _, ok := schema["created_at"]; ok {
			showFields = append(showFields, "created_at")
		}
		for _, field := range showFields {
			if field == "vid" {
				continue
			}
			if _, ok := schema[field]; !ok {
				return 0, nil, fmt.Errorf("invalid show field '%s'", field)
			}
			ngql += fmt.Sprintf("properties(vertex).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	}
	ngql = strings.TrimSuffix(ngql, ", ")

	// 查询总数
	totalGL := ngql + ";"
	slog.Debug(fmt.Sprintf("{nebula_client.LookupVertexes()} Total ngql: %s", ngql))
	totalRes, err := ConnectAndExcute(totalGL)
	if err != nil {
		return 0, nil, err
	}
	total := totalRes.GetRowSize()

	// 排序
	if _, ok := schema["created_at"]; ok && needOrder {
		ngql += " | ORDER BY $-.created_at ASC"
	}

	// 最后，分页
	if q.PageSize > 0 {
		index := 1
		if q.PageIndex > 1 {
			index = q.PageIndex
		}
		offset := (index - 1) * q.PageSize
		ngql += fmt.Sprintf(" | LIMIT %d, %d", offset, q.PageSize)
	}

	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.LookupVertexes()} ngql: %s", ngql))

	// 查询数据
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return 0, nil, err
	}

	// 解析数据到result
	dataTable := res.AsStringTable()
	result, err := ParseStringTable(dataTable, fields, schema)
	if err != nil {
		return 0, nil, err
	}

	return total, result, nil
}

// 沿着一个边，go one step，返回所有终点vertex的属性数据。
// 注意，schema参数，要求终点Vertex都关联了这个tag schema， 否则出现不可预料的错误（可能抛错，string类型不抛错且将得到UNKNOWN_PROP的值）。
// 如果数据模版schema定义了created_at字段，则对查询结果按created_at顺序排列。
func GoGetEndsData(startVid string, over string, schema map[string]string, showFields ...string) ([]map[string]interface{}, error) {
	// 构造ngql
	ngql := fmt.Sprintf("GO FROM \"%s\" OVER %s YIELD id($$) as VertexID, ", startVid, over)
	fields := []string{"VertexID"}
	if len(showFields) == 0 {
		for field := range schema {
			ngql += fmt.Sprintf("properties($$).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	} else if len(showFields) == 1 && showFields[0] == "vid" {
	} else {
		if _, ok := schema["created_at"]; ok {
			showFields = append(showFields, "created_at")
		}
		for _, field := range showFields {
			if field == "vid" {
				continue
			}
			if _, ok := schema[field]; !ok {
				return nil, fmt.Errorf("invalid show field '%s'", field)
			}
			ngql += fmt.Sprintf("properties($$).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	}
	ngql = strings.TrimSuffix(ngql, ", ")
	if _, ok := schema["created_at"]; ok {
		ngql += " | ORDER BY $-.created_at ASC"
	}
	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.GoGetEndsData()} ngql: %s", ngql))

	// 执行GO查询
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	// 解析数据
	stringTable := res.AsStringTable()
	dataTable, err := ParseStringTable(stringTable, fields, schema)
	if err != nil {
		return nil, err
	}

	return dataTable, nil
}

func GoGetEndsDataWithStep(startVid string, over string, step int, schema map[string]string, showFields ...string) ([]map[string]interface{}, error) {
	// 构造ngql
	ngql := fmt.Sprintf("GO %d STEPS FROM \"%s\" OVER %s YIELD id($$) as VertexID, ", step, startVid, over)
	fields := []string{"VertexID"}
	if len(showFields) == 0 {
		for field := range schema {
			ngql += fmt.Sprintf("properties($$).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	} else if len(showFields) == 1 && showFields[0] == "vid" {
	} else {
		if _, ok := schema["created_at"]; ok {
			showFields = append(showFields, "created_at")
		}
		for _, field := range showFields {
			if field == "vid" {
				continue
			}
			if _, ok := schema[field]; !ok {
				return nil, fmt.Errorf("invalid show field '%s'", field)
			}
			ngql += fmt.Sprintf("properties($$).%s as %s, ", field, field)
			fields = append(fields, field)
		}
	}
	ngql = strings.TrimSuffix(ngql, ", ")
	if _, ok := schema["created_at"]; ok {
		ngql += " | ORDER BY $-.created_at ASC"
	}
	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.GoGetEndsData()} ngql: %s", ngql))

	// 执行GO查询
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	// 解析数据
	stringTable := res.AsStringTable()
	dataTable, err := ParseStringTable(stringTable, fields, schema)
	if err != nil {
		return nil, err
	}

	return dataTable, nil
}

func GoGetRelations(startVid string, over []string, schema map[string]string, reverse bool, showFields ...string) ([]map[string]interface{}, error) {
	overEdge := "*"
	if len(over) != 0 {
		overEdge = strings.Join(over, ", ")
	}

	// 构造ngql
	ngql := fmt.Sprintf("GO FROM \"%s\" OVER %s YIELD type(edge) as edgeType, id($$) as vid, ", startVid, overEdge)
	if reverse {
		ngql = fmt.Sprintf("GO FROM \"%s\" OVER %s REVERSELY YIELD type(edge) as edgeType, id($$) as vid, ", startVid, overEdge)
	}
	// fields := []string{"vid", "edgeType"}
	fields := []string{"edgeType", "vid"}
	for _, field := range showFields {
		if field == "vid" {
			continue
		}
		if _, ok := schema[field]; !ok {
			return nil, fmt.Errorf("invalid show field '%s'", field)
		}
		ngql += fmt.Sprintf("properties($$).%s as %s, ", field, field)
		fields = append(fields, field)
	}
	ngql = strings.TrimSuffix(ngql, ", ")
	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.GoGetEndsData()} ngql: %s", ngql))

	// 执行GO查询
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	// 解析数据
	stringTable := res.AsStringTable()
	dataTable, err := ParseStringTable(stringTable, fields, schema)
	if err != nil {
		return nil, err
	}

	return dataTable, nil
}

// 沿指定边做遍历，删除末端所有节点。只遍历一层数据。
func GoAndDeleteEndVertex(startVid string, edge string) error {
	ngql := fmt.Sprintf("GO FROM \"%s\" OVER %s YIELD dst(edge) AS id | DELETE VERTEX $-.id;", startVid, edge)
	slog.Debug(fmt.Sprintf("{nebula_client.GoAndDeleteEndVertex()} ngql: %s", ngql))
	_, err := ConnectAndExcute(ngql)
	if err != nil {
		return err
	}
	return nil
}
func GoAndDeleteEndVertexByStep(startVid string, edge string, step int) error {
	if step <= 0 {
		step = 1
	}
	ngql := fmt.Sprintf("GO %d STEPS FROM \"%s\" OVER %s YIELD dst(edge) AS id | DELETE VERTEX $-.id;", step, startVid, edge)
	slog.Debug(fmt.Sprintf("{nebula_client.GoAndDeleteEndVertexByStep()} ngql: %s", ngql))
	_, err := ConnectAndExcute(ngql)
	if err != nil {
		return err
	}
	return nil
}

// GoAndCheckNextVertexExist() 与 GoAndCheckUpwardVertexExist() 的公共部分
func goAndCheckVertexExist(startVid string, edge string, reverse bool, keepNum int) (bool, error) {
	if err := ValidateName(startVid); err != nil {
		return false, err
	}
	if err := ValidateName(edge); err != nil {
		return false, err
	}
	ngql := fmt.Sprintf("GO FROM \"%s\" OVER %s YIELD dst(edge) AS vid | LIMIT %d;", startVid, edge, keepNum+1)
	if reverse {
		ngql = fmt.Sprintf("GO FROM \"%s\" OVER %s REVERSELY YIELD dst(edge) AS vid | LIMIT %d;", startVid, edge, keepNum+1)
	}
	slog.Debug(fmt.Sprintf("{nebula_client.GoAndCheckNextVertexExist()} ngql: %s", ngql))
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return false, err
	}
	if res.GetRowSize() > keepNum {
		return true, nil
	}
	return false, nil
}

// 沿指定边做遍历，检查是否有末端节点存在，存在返回true。只遍历一层数据。
func GoAndCheckNextVertexExist(startVid string, edge string, keepNums ...int) (bool, error) {
	keepNum := 0
	if len(keepNums) >= 1 {
		keepNum = keepNums[0]
	}
	return goAndCheckVertexExist(startVid, edge, false, keepNum)
}

// 沿指定边做逆向查询，检查是否有上游节点存在，存在返回true。只遍历一层数据。
func GoAndCheckUpwardVertexExist(startVid string, edge string, keepNums ...int) (bool, error) {
	keepNum := 0
	if len(keepNums) >= 1 {
		keepNum = keepNums[0]
	}
	return goAndCheckVertexExist(startVid, edge, true, keepNum)
}

func LookupAndCheckVertexExist(tag string, keepNums ...int) (bool, error) {
	keepNum := 0
	if len(keepNums) >= 1 {
		keepNum = keepNums[0]
	}
	ngql := fmt.Sprintf("LOOKUP ON %s | LIMIT %d;", tag, keepNum+1)
	slog.Debug(fmt.Sprintf("{nebula_client.LookupAndCheckVertexExist()} ngql: %s", ngql))
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return false, err
	}
	if res.GetRowSize() > keepNum {
		return true, nil
	}
	return false, nil
}

// 用于根据唯一性要求的字段，检查是否有重复的数据。
// 返回重复的vertex数量
func LookupVertexToCheckDuplicatedByProps(tds DataSchema, UniqueFields []string, exceptVids ...string) ([]map[string]interface{}, error) {
	if len(UniqueFields) <= 0 {
		panic("nebula_client.LookupVertexToCheckDuplicatedByProps(): Usage error. Param 'UniqueFields' should not be empty.")
	}
	tag, schema := tds.GetSchema()
	data := tds.GetDataInSchema()
	ngql := fmt.Sprintf("LOOKUP ON %s WHERE ", tag)
	// if
	for _, field := range UniqueFields {
		fieldDef := schema[field]
		fieldVal := data[field]
		nVal, err := getNGQLValue(fieldDef, fieldVal)
		if err != nil {
			return nil, err
		}
		ngql += fmt.Sprintf("%s.%s == %s OR ", tag, field, nVal)
	}
	ngql = strings.TrimSuffix(ngql, " OR ")
	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.LookupAndCheckVertexExist()} ngql: %s", ngql))
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	dataTable := res.AsStringTable()
	result, err := ParseStringTable(dataTable, []string{"VertexID"}, schema)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func GetTagNameByVid(vid string) ([]string, error) {
	ngql := fmt.Sprintf("FETCH PROP ON * '%s' YIELD tags(vertex) as tgs;", vid)
	slog.Debug(fmt.Sprintf("{nebula_client.GetVertexTagName()} ngql: %s", ngql))
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	// 解析查询结果
	fields := []string{"VertexID", "tgs"}
	schema := map[string]string{
		"tgs": "list",
	}

	dataTable := res.AsStringTable()
	resData, err := ParseStringTable(dataTable, fields, schema)
	if err != nil {
		return nil, err
	}
	if len(resData) == 0 {
		return nil, fmt.Errorf("no data found with vid '%s'", vid)
	}
	tagsI := resData[0]["tgs"].([]interface{})
	result := []string{}
	for _, val := range tagsI {
		valStr := val.(string)
		result = append(result, valStr)
	}
	return result, nil
}
