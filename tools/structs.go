package tools

import (
	"time"
)

// --------------------- Null 类型，不占内存空间 ---------------------
type Null struct{}

// --------------------- 自定义json序列化格式的time类型 ---------------------

const SIMPLE_TIME_LAYOUT = "2006-01-02 15:04:05"

type SimpleTime time.Time

func (t *SimpleTime) UnmarshalJSON(b []byte) error {
	parsed, err := time.Parse(`"`+SIMPLE_TIME_LAYOUT+`"`, string(b))
	if err != nil {
		return err
	}
	*t = SimpleTime(parsed)
	return nil
}

func (t SimpleTime) MarshalJSON() ([]byte, error) {
	s := time.Time(t).Format(SIMPLE_TIME_LAYOUT)
	return []byte("\"" + s + "\""), nil
}

func (t SimpleTime) String() string {
	return time.Time(t).Format(SIMPLE_TIME_LAYOUT)
}
