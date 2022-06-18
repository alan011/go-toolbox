package ginstarter

import (
	"io/ioutil"

	"github.com/gin-gonic/gin"
)

func GetRawJSON(ctx *gin.Context) string {
	dataB, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		panic(err)
	}
	return string(dataB)
}
