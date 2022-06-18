package redistarter

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
)

const DEFAULT_IDLE_SIZE = 10
const DEFAULT_IDLE_TIMEOUT = 300 * time.Second

var Pool *redis.Pool

func dial(network, address, password string) (redis.Conn, error) {
	c, err := redis.Dial(network, address)
	if err != nil {
		return nil, err
	}
	if password != "" {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, err
}

// 用途：初始化一个全局redis连接池
// 特别参数：
//     idleConnections: 0表示使用默认值'10'
//     idleTimeout: 0表示使用默认值300s
func Init(address, password string, idleConnections int, idleTimeout time.Duration) {
	// 处理参数默认值
	connections := idleConnections
	if connections <= 0 {
		connections = DEFAULT_IDLE_SIZE
	}

	timeout := idleTimeout
	if timeout <= 0 {
		timeout = DEFAULT_IDLE_TIMEOUT
	}

	// 初始化连接池
	if Pool == nil {
		Pool = &redis.Pool{
			MaxIdle:     connections,
			IdleTimeout: timeout,
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
			Dial: func() (redis.Conn, error) {
				return dial("tcp", address, password)
			},
		}
	}
}

// 便捷方法: 根据指定的key，从redis中获取一个string类型的value
func GetStrVal(key string) (string, error) {
	if Pool == nil {
		return "", errors.New("GetStrval(): redis client pool is not initiallized")
	}

	conn := Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return "", err
	}
	data, err := conn.Do("GET", key)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", nil // no data was associated with this key
	}
	b, err := redis.Bytes(data, err)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// 便捷方法: 根据指定的key，从redis中获取一个string类型的value
func SetStrVal(key string, val string) error {
	if Pool == nil {
		return errors.New("GetStrval(): redis client pool is not initiallized")
	}

	conn := Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	_, err := conn.Do("SET", key, val)
	return err
}
