package nebula_client

import (
	"errors"
	"fmt"
	"strings"

	slog "codeops.didachuxing.com/lordaeron/go-toolbox/simplelog"
)

// 生成INSERT VERTEX语句
func getInsertEdgeNGQL(edge string, schema map[string]string, data map[string]interface{}) (string, error) {
	ngqlPrefix := "INSERT EDGE"

	src_vid := data["src_vid"].(string)
	dst_vid := data["dst_vid"].(string)

	edgeTypeNGQL := fmt.Sprintf("%s(", edge)
	edgeNGQL := fmt.Sprintf("\"%s\"->\"%s\":(", src_vid, dst_vid)
	for field, fieldDef := range schema {
		edgeTypeNGQL = fmt.Sprintf("%s%s, ", edgeTypeNGQL, field)

		fieldVal, ok := data[field]
		if !ok {
			continue
		}
		fieldValNGQL, err := getNGQLValue(fieldDef, fieldVal)
		if err != nil {
			return "", err
		}
		edgeNGQL = fmt.Sprintf("%s%s, ", edgeNGQL, fieldValNGQL)
	}
	edgeTypeNGQL = strings.TrimSuffix(edgeTypeNGQL, ", ") + ")"
	edgeNGQL = strings.TrimSuffix(edgeNGQL, ", ") + ")"

	ngql := fmt.Sprintf("%s %s VALUES %s;", ngqlPrefix, edgeTypeNGQL, edgeNGQL)
	return ngql, nil
}

func InsertEdge(eds DataSchema) error {
	// 校验数据
	edge, schema := eds.GetSchema()
	data := eds.GetDataInSchema()
	if err := ValidateEdge(edge, schema, data); err != nil {
		return err
	}

	// 构造ngql语句
	src_vid := data["src_vid"].(string)
	dst_vid := data["dst_vid"].(string)
	ngql, err := getInsertEdgeNGQL(edge, schema, data)
	if err != nil {
		return err
	}
	slog.Debug(fmt.Sprintf("{nebula_client.InsertEdge()} ngql: %s", ngql))

	// 连接DB，执行ngql语句
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to Insert Edge '%s'->'%s'. %s.", src_vid, dst_vid, err.Error())
		return errors.New(msg)
	}
	return nil
}

func InsertEmptyEdge(edgeType string, src string, dst string) error {
	ngql := fmt.Sprintf("INSERT EDGE %s() VALUES '%s' -> '%s':();", edgeType, src, dst)
	slog.Debug(fmt.Sprintf("{nebula_client.InsertEmptyEdge()} ngql: %s", ngql))
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to Insert Edge '%s'->'%s'. %s.", src, dst, err.Error())
		return errors.New(msg)
	}
	return nil
}

func DeleteEdgeBySchema(edgeType string, src string, dst string) error {
	ngql := fmt.Sprintf("DELETE EDGE %s '%s' -> '%s';", edgeType, src, dst)
	slog.Debug(fmt.Sprintf("{nebula_client.DeleteEdgeBySchema()} ngql: %s", ngql))
	if _, err := ConnectAndExcute(ngql); err != nil {
		msg := fmt.Sprintf("Failed to Delete Edge '%s'->'%s'. %s.", src, dst, err.Error())
		return errors.New(msg)
	}
	return nil
}

func FetchEdgeData(eds DataSchema) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func LookupEdges(edgeType string) ([]map[string]interface{}, error) {
	ngql := fmt.Sprintf("LOOKUP ON %s;", edgeType)
	slog.Debug("{nebula_client.LookupEdges()} ngql: " + ngql)
	res, err := ConnectAndExcute(ngql)
	if err != nil {
		return nil, err
	}

	// 解析数据
	fields := []string{"SrcVID", "DstVID"}
	schema := map[string]string{
		"SrcVID": "string",
		"DstVID": "string",
	}
	stringTable := res.AsStringTable()
	dataTable, err := ParseStringTable(stringTable, fields, schema)
	if err != nil {
		return nil, err
	}

	return dataTable, nil
}
