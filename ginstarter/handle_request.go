package ginstarter

import (
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"
)

// 解析RESTAPI URL PATH中的ID参数
func ParseID(ctx *gin.Context) (int, error) {
	msg := "url path with invalid 'id'"
	i, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		return 0, errors.New(msg)
	}
	if i <= 0 {
		return 0, errors.New(msg)
	}
	return int(i), nil
}
