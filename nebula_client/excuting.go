package nebula_client

import (
	"errors"
	"fmt"
	"strings"

	nebula "github.com/vesoft-inc/nebula-go/v2"
)

func executeNqgl(session *nebula.Session, ngql string) (*nebula.ResultSet, error) {
	space_nqgl := fmt.Sprintf("USE %s; ", NebulaSpace)
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ngql)), "USE") {
		ngql = space_nqgl + ngql
	}

	// fmt.Printf("Executing NGQL:\t%s\n", ngql)
	res, err := session.Execute(ngql)
	if err != nil {
		msg := fmt.Sprintf("NGQL executing Failed. %s", err.Error())
		return nil, errors.New(msg)
	}
	if !res.IsSucceed() {
		msg := fmt.Sprintf("NGQL executing failed. ErrorCode: %v. ErrorMsg: %s", res.GetErrorCode(), res.GetErrorMsg())
		return nil, errors.New(msg)
	}
	return res, nil
}

// 执行单条ngql语句。
func ConnectAndExcute(ngql string) (*nebula.ResultSet, error) {
	// 获取连接session
	session, err := Pool.GetSession(NebulaUser, NebulaPassword)
	if err != nil {
		msg := fmt.Sprintf("Nebula DB Connecting Error. %s", err.Error())
		return nil, errors.New(msg)
	}
	defer session.Release()

	// 执行ngql
	return executeNqgl(session, ngql)
}

// 批量执行ngql语句。注意，无法保证原子性。
func ConnectAndBatchExcute(ngqls []string) ([]*nebula.ResultSet, error) {
	// 获取连接session
	session, err := Pool.GetSession(NebulaUser, NebulaPassword)
	if err != nil {
		msg := fmt.Sprintf("Nebula DB Connecting Error. %s", err.Error())
		return nil, errors.New(msg)
	}
	defer session.Release()

	// 逐条执行，出错立即返回。
	results := []*nebula.ResultSet{}
	for _, ql := range ngqls {
		res, err := executeNqgl(session, ql)
		results = append(results, res)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}
