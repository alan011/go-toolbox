package config

/*

要求一个配置文件`conf/config.yml`

*/

import (
	"io/fs"
	"strings"

	"codeops.didachuxing.com/lordaeron/go-toolbox/tools"
)

const DEFAULT_CONFIG_PATH = "conf/config.yaml"
const DEFAULT_LISTEN_ADDR = ":8000"
const DEFAULT_LOG_LEVEL = "INFO"
const DEFAULT_DEBUG_MOD = false

var LogLevel string
var DebugMod bool
var ListenAddr string
var AuthRedirectURL string

var RawData map[string]interface{}

func Parse() error {
	RawData = map[string]interface{}{}
	err := tools.ParseYaml(DEFAULT_CONFIG_PATH, &RawData)
	if err != nil {
		// 配件文件不存在时，加载默认配置，不报错。
		if _, ok := err.(*fs.PathError); !ok {
			return err
		}
	}

	// 设置LogLevel
	if val, ok := RawData["log_level"].(string); ok {
		LogLevel = strings.ToUpper(val)
	} else {
		LogLevel = DEFAULT_LOG_LEVEL
	}

	// 设置DebugMod
	if LogLevel == "DEBUG" {
		DebugMod = true
	}

	// 设置ListenAddr
	if val, ok := RawData["listen_addr"].(string); ok {
		ListenAddr = val
	} else {
		ListenAddr = DEFAULT_LISTEN_ADDR
	}

	// 设置AuthRedirectURL
	if val, ok := RawData["auth_redirect_url"].(string); ok {
		AuthRedirectURL = val
	}
	return nil
}
