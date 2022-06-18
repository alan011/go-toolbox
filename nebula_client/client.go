package nebula_client

import (
	"errors"
	"fmt"
	"strings"

	"codeops.didachuxing.com/lordaeron/go-toolbox/config"

	nebula "github.com/vesoft-inc/nebula-go/v2"
)

var (
	NebulaHosts    []string
	NebulaPort     int
	NebulaUser     string
	NebulaPassword string
	NebulaSpace    string

	Pool *nebula.ConnectionPool
)

// 解析原始配置数据
func getNebulaConfig() error {
	// nebula配置是个json字典，对应实际数据类型应该是个`map[interface{}]interface{}`
	nebulaConfigRaw, _ := config.RawData["nebula_info"].(map[interface{}]interface{})
	if nebulaConfigRaw == nil {
		return errors.New(`cannot get 'nebula_info' from config`)
	}

	// 依次对key，val做interface类型判断。填充配置信息全局变量。
	for k, v := range nebulaConfigRaw {
		switch key := k.(string); key {
		case "host":
			hostStr, _ := v.(string)
			if hostStr == "" {
				return errors.New(`invalid nebula host config`)
			}
			hosts := strings.Split(hostStr, ",")
			for _, val := range hosts {
				host := strings.TrimSpace(val)
				if host == "" {
					return errors.New(`invalid nebula host config, empty string found`)
				}
				NebulaHosts = append(NebulaHosts, host)
			}
			if len(NebulaHosts) <= 0 {
				return errors.New(`invalid nebula host config, no effective nebula host configured`)
			}

		case "port":
			NebulaPort, _ = v.(int)
			if NebulaPort <= 0 {
				return errors.New(`invalid nebula port config`)
			}
		case "user":
			val, _ := v.(string)
			NebulaUser = strings.TrimSpace(val)
			if NebulaUser == "" {
				return errors.New(`invalid nebula user config`)
			}
		case "password":
			NebulaPassword, _ = v.(string)
		case "space":
			val, _ := v.(string)
			NebulaSpace = strings.TrimSpace(val)
			if NebulaSpace == "" {
				return errors.New(`invalid nebula space config`)
			}
		default:
			return fmt.Errorf(`unrecorginzed field '%s' in 'nebula_info' config`, key)
		}
	}

	return nil
}

// 初始化nebula客户端连接池
func Init() {
	// 从config.RawData中解析nebula数据库连接信息配置。
	err := getNebulaConfig()
	if err != nil {
		panic(fmt.Sprintf("Nebula Config not Correct. %s.", err.Error()))
	}

	// 初始化连接池
	var hostList []nebula.HostAddress
	for _, host := range NebulaHosts {
		hostAddr := nebula.HostAddress{Host: host, Port: NebulaPort}
		hostList = append(hostList, hostAddr)
	}
	poolConfig := nebula.GetDefaultConf()
	Pool, err = nebula.NewConnectionPool(hostList, poolConfig, nil)
	if err != nil {
		panic(fmt.Sprintf("Fail to initialize the connection pool, host: %s, port: %d. %s.", NebulaHosts, NebulaPort, err.Error()))
	}
}
