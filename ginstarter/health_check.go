package ginstarter

import "github.com/gin-gonic/gin"

func healthCheck(ctx *gin.Context) {
	Success(ctx, 200, gin.H{"msg": "OK"})
}
