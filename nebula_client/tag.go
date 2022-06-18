package nebula_client

import (
	"errors"
	"fmt"
	"strings"

	slog "codeops.didachuxing.com/lordaeron/go-toolbox/simplelog"
)

const TAG_BASIC_INDEX_SUFFIX = "_index_0"

func CreateTagBySchema(tag string, schema map[string]string, ifNotExists bool, toCreateIndex bool) error {
	// 不允许没有属性的空tag
	if len(schema) <= 0 {
		return errors.New("invalid tag schema")
	}

	// 构造ngql语句
	ngql := "CREATE TAG "
	if ifNotExists {
		ngql += "IF NOT EXISTS "
	}
	ngql += tag + "("
	for field, fieldDef := range schema {
		field = strings.TrimSpace(field)
		fieldDef = strings.TrimSpace(fieldDef)
		if field == "" || fieldDef == "" {
			return errors.New("tag field or fieldDef cannot be empty")
		}
		fieldDefs := strings.Split(fieldDef, " ")
		fieldType := strings.ToLower(fieldDefs[0])
		if fieldType == "dict" || fieldType == "list" {
			fieldDefs[0] = "string"
			fieldDef = strings.Join(fieldDefs, " ")
		}
		ngql += field + " " + fieldDef + ", "
	}
	ngql = strings.TrimSuffix(ngql, ", ") + ");"
	slog.Debug(fmt.Sprintf("{nebula_client.CreateTagBySchema()} ngql: %s", ngql))

	// 连接DB，执行ngql语句
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to create tag '%s'. %s.", tag, err.Error())
		return errors.New(msg)
	}

	// 创建索引 (nebula-go的connection pool管理，有自带的session重用机制，故，不用担心过多的创建DB连接)
	if toCreateIndex {
		tagIndexName := tag + TAG_BASIC_INDEX_SUFFIX
		ingql := fmt.Sprintf("CREATE TAG INDEX IF NOT EXISTS %s ON %s();", tagIndexName, tag)
		if _, err := ConnectAndExcute(ingql); err != nil {
			msg := fmt.Sprintf("Failed to create tag index '%s'. %s.", tagIndexName, err.Error())
			return errors.New(msg)
		}
	}

	return nil
}

func isPropDefChanged(oldDef string, newDef string) (changed bool) {
	changed = false

	strClean := func(s string) string {
		subs := strings.Split(strings.ToUpper(strings.TrimSpace(s)), " ")
		cleans := []string{}
		for _, item := range subs {
			if item != "" {
				cleans = append(cleans, item)
			}
		}
		return strings.Join(cleans, " ")
	}

	oldClean := strClean(oldDef)
	newClean := strClean(newDef)

	return oldClean != newClean
}

func AlterTagBySchema(tagName string, oldSchema map[string]string, newSchema map[string]string) (bool, error) {
	addProps := []string{}
	changeProps := []string{}
	dropProps := []string{}

	// 将dict与list类型的字段，转化为string类型的ngql
	checkDef := func(fieldDef string) string {
		fieldDefs := strings.Split(fieldDef, " ")
		fieldType := strings.ToLower(fieldDefs[0])
		if fieldType == "dict" || fieldType == "list" {
			fieldDefs[0] = "string"
			fieldDef = strings.Join(fieldDefs, " ")
		}
		return fieldDef
	}

	// 查找需要新增、修改的属性。
	for newProp, newPropDef := range newSchema {
		oldPropDef, ok := oldSchema[newProp]
		if !ok {
			newPropDef = checkDef(newPropDef)
			subngql := fmt.Sprintf("%s %s", newProp, newPropDef)
			addProps = append(addProps, subngql)
		} else if isPropDefChanged(oldPropDef, newPropDef) {
			newPropDef = checkDef(newPropDef)
			subngql := fmt.Sprintf("%s %s", newProp, newPropDef)
			changeProps = append(changeProps, subngql)
		}
	}

	// 查找需要删除的属性
	for oldProp := range oldSchema {
		if _, ok := newSchema[oldProp]; !ok {
			dropProps = append(dropProps, oldProp)
		}
	}

	// 无变化就不做变更。
	if len(addProps) == 0 && len(changeProps) == 0 && len(dropProps) == 0 {
		return false, nil
	}

	// 构造ngql
	ngql := fmt.Sprintf("ALTER TAG %s", tagName)
	if len(addProps) > 0 {
		ngql += " ADD ("
		for _, subngql := range addProps {
			ngql += subngql + ", "
		}
		ngql = strings.TrimSuffix(ngql, ", ") + ")"
	}
	if len(changeProps) > 0 {
		if len(addProps) > 0 {
			ngql += ", CHANGE ("
		} else {
			ngql += " CHANGE ("
		}
		for _, subngql := range changeProps {
			ngql += subngql + ", "
		}
		ngql = strings.TrimSuffix(ngql, ", ") + ")"
	}
	if len(dropProps) > 0 {
		if len(addProps) > 0 || len(changeProps) > 0 {
			ngql += ", DROP ("
		} else {
			ngql += " DROP ("
		}
		for _, subngql := range dropProps {
			ngql += subngql + ", "
		}
		ngql = strings.TrimSuffix(ngql, ", ") + ")"
	}
	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.AlterTagBySchema()} ngql: %s", ngql))

	// 执行ngql
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to alter tag '%s'. %s.", tagName, err.Error())
		return false, errors.New(msg)
	}

	return true, nil
}

func CreateTag(tds DataSchema, ifNotExists bool, toCreateIndex bool) error {
	tag, schema := tds.GetSchema()
	return CreateTagBySchema(tag, schema, ifNotExists, toCreateIndex)
}

// 获取根tag名称匹配的所有index名称列表。
func GetTagIndexNames(tag string) ([]string, error) {
	ngql := "SHOW TAG INDEXES;"
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}
	tagIndexNames := []string{}
	for _, row := range res.AsStringTable()[1:] {
		tagName := strings.TrimSuffix(strings.TrimPrefix(row[1], "\""), "\"")
		tagIndexName := strings.TrimSuffix(strings.TrimPrefix(row[0], "\""), "\"")
		if tagName == tag {
			tagIndexNames = append(tagIndexNames, tagIndexName)
		}
	}
	return tagIndexNames, nil
}

func DropTagIndex(index string) error {
	ngql := fmt.Sprintf("DROP TAG INDEX %s;", index)
	slog.Debug(fmt.Sprintf("{nebula_client.DropTagIndex()} ngql: %s", ngql))
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to  Drop Tag Index '%s'. %s.", index, err.Error())
		return errors.New(msg)
	}
	return nil
}

// 删除tag，同时会删除与其相关的所有index索引。
func DropTagByName(tag string, ifExists bool) error {
	// 构造ngql语句
	ngql := "DROP TAG "
	if ifExists {
		ngql += "IF EXISTS "
	}
	ngql += tag + ";"

	// 删除对应的Tag Index
	indexNames, err := GetTagIndexNames(tag)
	if err != nil {
		return err
	}
	for _, indexName := range indexNames {
		if err := DropTagIndex(indexName); err != nil {
			return err
		}
	}

	// 连接DB，执行ngql语句
	slog.Debug(fmt.Sprintf("{nebula_client.DropTag()} ngql: %s", ngql))
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to  Drop Tag '%s'. %s.", tag, err.Error())
		return errors.New(msg)
	}
	return nil
}
