package nebula_client

import (
	"errors"
	"fmt"
	"strings"

	slog "codeops.didachuxing.com/lordaeron/go-toolbox/simplelog"
)

const EDGE_BASIC_INDEX_SUFFIX = "_index_0"

func CreateEdgeType(eds DataSchema, ifNotExists bool, toCreateIndex bool) error {
	edgeName, schema := eds.GetSchema()
	// 校验edgeName是否合法
	if err := ValidateName(edgeName); err != nil {
		return err
	}

	// 构造ngql语句
	ngql := "CREATE EDGE "
	if ifNotExists {
		ngql += "IF NOT EXISTS "
	}
	ngql += edgeName + "("
	for field, fieldType := range schema {
		if field == "" || fieldType == "" {
			return errors.New("edge prop field or fieldtype cannot be empty")
		}
		ngql += field + " " + fieldType + ", "
	}
	ngql = strings.TrimSuffix(ngql, ", ") + ");"
	slog.Debug(fmt.Sprintf("{nebula_client.CreateEdgeType()} ngql: %s", ngql))

	// 连接DB，执行ngql语句
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to create edge type '%s'. %s.", edgeName, err.Error())
		return errors.New(msg)
	}

	// 创建edge索引
	if toCreateIndex {
		edgeIndexName := edgeName + EDGE_BASIC_INDEX_SUFFIX
		ingql := fmt.Sprintf("CREATE EDGE INDEX IF NOT EXISTS %s ON %s();", edgeIndexName, edgeName)
		if _, err := ConnectAndExcute(ingql); err != nil {
			msg := fmt.Sprintf("Failed to create edge index '%s'. %s.", edgeIndexName, err.Error())
			return errors.New(msg)
		}
	}

	return nil
}

func AlterEdgeTypeBySchema(edgeName string, oldSchema map[string]string, newSchema map[string]string) (bool, error) {
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
	ngql := fmt.Sprintf("ALTER EDGE %s", edgeName)
	if len(addProps) > 0 {
		ngql += " ADD ("
		for _, subngql := range addProps {
			ngql += subngql + ", "
		}
		ngql = strings.TrimSuffix(ngql, ", ") + ")"
	}
	if len(changeProps) > 0 {
		ngql += " CHANGE ("
		for _, subngql := range changeProps {
			ngql += subngql + ", "
		}
		ngql = strings.TrimSuffix(ngql, ", ") + ")"
	}
	if len(dropProps) > 0 {
		ngql += " DROP ("
		for _, subngql := range dropProps {
			ngql += subngql + ", "
		}
		ngql = strings.TrimSuffix(ngql, ", ") + ")"
	}
	ngql += ";"
	slog.Debug(fmt.Sprintf("{nebula_client.AlterEdgeTypeBySchema()} ngql: %s", ngql))

	// 执行ngql
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to alter edgeType '%s'. %s.", edgeName, err.Error())
		return false, errors.New(msg)
	}

	return true, nil
}

// 获取根edge名称匹配的所有index名称列表。
func GetEdgeIndexNames(edge string) ([]string, error) {
	ngql := "SHOW EDGE INDEXES;"
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}
	edgeIndexNames := []string{}
	for _, row := range res.AsStringTable()[1:] {
		edgeName := strings.TrimSuffix(strings.TrimPrefix(row[1], "\""), "\"")
		edgeIndexName := strings.TrimSuffix(strings.TrimPrefix(row[0], "\""), "\"")
		if edgeName == edge {
			edgeIndexNames = append(edgeIndexNames, edgeIndexName)
		}
	}
	return edgeIndexNames, nil
}

func DropEdgeIndex(index string) error {
	ngql := fmt.Sprintf("DROP EDGE INDEX %s;", index)
	slog.Debug(fmt.Sprintf("{nebula_client.DropEdgeIndex()} ngql: %s", ngql))
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to Drop Edge Index '%s'. %s.", index, err.Error())
		return errors.New(msg)
	}
	return nil
}

func DropEdgeType(eds DataSchema, ifExists bool) error {
	edgeName, _ := eds.GetSchema()

	// 先drop index
	// indexNames, err := GetEdgeIndexNames(edgeName)
	// if err != nil {
	// 	return err
	// }
	// for _, indexName := range indexNames {
	// 	if err := DropEdgeIndex(indexName); err != nil {
	// 		return err
	// 	}
	// }

	// 构造ngql语句
	ngql := "DROP EDGE "
	if ifExists {
		ngql += "IF EXISTS "
	}
	ngql += edgeName + ";"

	// 连接DB，执行ngql语句
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to drop edge type '%s'. %s.", edgeName, err.Error())
		return errors.New(msg)
	}
	return nil
}
