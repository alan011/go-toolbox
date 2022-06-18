package dbstarter

import (
	"errors"
	"fmt"
	"time"

	"codeops.didachuxing.com/lordaeron/go-toolbox/config"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const DEFAULT_DB_PORT = 3306
const DEFAULT_CHARSET = "utf8mb4"

var (
	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string
	DBCharset  string

	DB *gorm.DB
)

func Init() error {
	// 解析配置。
	var (
		ok1 bool
		ok2 bool
		ok3 bool
		ok4 bool
		ok5 bool
		// ok bool
	)
	DBHost, ok1 = config.RawData["db_host"].(string)
	DBPort, ok2 = config.RawData["db_port"].(int)
	DBName, ok3 = config.RawData["db_name"].(string)
	DBUser, ok4 = config.RawData["db_user"].(string)
	DBPassword, ok5 = config.RawData["db_password"].(string)
	if !(ok1 && ok2 && ok3 && ok4 && ok5) {
		return errors.New("invalid db config")
	}
	if DBHost == "" || DBName == "" || DBUser == "" {
		return errors.New("invalid db config. db_host, db_name, db_user cannot be empty")
	}
	if DBPort <= 0 {
		DBPort = DEFAULT_DB_PORT
	}
	if charset, ok := config.RawData["db_charset"].(string); ok && charset != "" {
		DBCharset = charset
	} else {
		DBCharset = DEFAULT_CHARSET
	}

	// 初始化数据库连接池
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local", DBUser, DBPassword, DBHost, DBPort, DBName, DBCharset)
	var err error
	DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	// 连接池参数设置
	dbpool, err := DB.DB()
	if err != nil {
		return err
	}
	dbpool.SetMaxIdleConns(20)                  // 最大空闲连接数
	dbpool.SetConnMaxLifetime(30 * time.Minute) //最大复用时间
	// dbpool.SetMaxOpenConns(100)          // 最大连接数限制。
	return nil
}
