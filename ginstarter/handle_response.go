package ginstarter

import (
	"encoding/json"
	"fmt"

	slog "codeops.didachuxing.com/lordaeron/go-toolbox/simplelog"

	"github.com/gin-gonic/gin"
)

func handleJsonBody(data gin.H, result bool, status_code int, ctx *gin.Context) {
	// 测试一下，需不需要加map的互斥锁
	if _, ok := data["result"]; !ok {
		data["result"] = result
	}
	if _, ok := data["code"]; !ok {
		data["code"] = status_code
	}
	if _, ok := data["msg"]; !ok {
		data["msg"] = ""
	}

	if !data["result"].(bool) {
		slog.Error(fmt.Sprintf("%v\n", data))
	} else if LogLevel == "DEBUG" {
		datab, _ := json.MarshalIndent(data, "", "  ")
		datas := string(datab)
		slog.Debug(datas)
	}

	ctx.JSON(status_code, data)
}

func Success(ctx *gin.Context, status_code int, data gin.H) {
	if status_code >= 300 {
		slog.Panic("ginstarter.Success(): Usage error.")
	}
	handleJsonBody(data, true, status_code, ctx)
}

func Failed(ctx *gin.Context, status_code int, data gin.H) {
	if status_code < 400 {
		slog.Panic("ginstarter.Failed(): Usage error.")
	}
	handleJsonBody(data, false, status_code, ctx)
}
