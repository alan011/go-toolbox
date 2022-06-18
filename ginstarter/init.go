package ginstarter

import (
	"strings"

	"codeops.didachuxing.com/lordaeron/go-toolbox/config"
	slog "codeops.didachuxing.com/lordaeron/go-toolbox/simplelog"

	"fmt"

	"github.com/gin-gonic/gin"
)

var LogLevel string

func MakeEngine() *gin.Engine {
	slog.Debug("Gin engine initiallizing...")
	if !config.DebugMod {
		gin.SetMode(gin.ReleaseMode)
	}
	gin.DisableConsoleColor()

	engine := gin.New()
	engine.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("%s [GIN-ACCESS] %s \"%s %s %s %d %s %s %s\"\n",
			param.TimeStamp.Format("2006/01/02 15:04:05"),
			param.ClientIP,
			param.Method,
			param.Path,
			param.Request.Proto,
			param.StatusCode,
			param.Latency,
			param.Request.UserAgent(),
			param.ErrorMessage,
		)
	}))
	engine.Use(gin.Recovery())

	// Other middlewares.

	// 注册默认的healch check方法
	engine.GET("/health", healthCheck)

	return engine
}

func Init() *gin.Engine {
	// 加载配置文件
	err := config.Parse()
	if err != nil {
		panic(fmt.Sprintf("ERROR: Fail To load config file. %s", err.Error()))
	}
	// 初始化一个全局SimpleLog
	LogLevel = strings.ToUpper(config.LogLevel)
	slog.SlogInit(config.LogLevel, "GinStarter.Init(): ")
	defer slog.RemovePrefix()
	slog.Info("Config Data loaded successfully.")

	// 初始化gin引擎
	return MakeEngine()
}
