package requests

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"codeops.didachuxing.com/lordaeron/go-toolbox/tools"
)

// 目前只支持json格式的数据交互, 即，默认会添加Header, "Content-Type: application/json"
// 即：Data提供的数据，需要能正确的转化为一个json字符串。
// Params，用于生成url查询参数。
type Requests struct {
	Url       string
	Method    string
	Headers   map[string]string
	QueryData map[string]interface{}
	Data      interface{}
	Timeout   time.Duration

	// flag
	DoNotReadBody bool

	// result
	Response   *http.Response
	StatusCode int
	Text       string

	// 私有方法
	isResponseBodyClosed bool
}

func (req *Requests) SetRequestHeader(newReq *http.Request) {
	newReq.Header.Set("Content-Type", "application/json")
	for key, val := range req.Headers {
		newReq.Header.Set(key, val)
	}
}

func (req *Requests) PackQueryData(newReq *http.Request) {
	addInterfaceVal := func(q *url.Values, key string, val interface{}) {
		if val == nil {
			q.Add(key, "null")
		} else {
			q.Add(key, fmt.Sprintf("%v", val))
		}
	}

	q := newReq.URL.Query()
	for key, val := range req.QueryData {
		if valSlice, ok := val.([]interface{}); ok {
			for _, item := range valSlice {
				addInterfaceVal(&q, key, item)
			}
			continue
		}
		addInterfaceVal(&q, key, val)
	}
	newReq.URL.RawQuery = q.Encode()
}

// 请求结果，将写入Requests.Response中。
func (req *Requests) Request() error {
	var bodyB []byte
	var err error
	var body io.Reader
	var nullVal = tools.Null{}
	var supportedMethods = map[string]tools.Null{
		"GET":    nullVal,
		"HEAD":   nullVal,
		"POST":   nullVal,
		"PUT":    nullVal,
		"PATCH":  nullVal,
		"DELETE": nullVal,
	}

	// 检查参数
	if !strings.HasPrefix(req.Url, "http://") && !strings.HasPrefix(req.Url, "https://") {
		return fmt.Errorf("invalid url: %s", req.Url)
	}
	method := strings.ToUpper(req.Method)
	if _, ok := supportedMethods[method]; !ok {
		return fmt.Errorf("unsupported http method: %s", req.Method)
	}

	// 生成body数据
	if req.Data != nil {
		bodyB, err = json.Marshal(req.Data)
		if err != nil {
			return fmt.Errorf("illegal data. %s", err.Error())
		}
		body = strings.NewReader(string(bodyB))
	}

	// 构造一个http.Request
	newReq, err := http.NewRequest(method, req.Url, body)
	if err != nil {
		return fmt.Errorf("failed to make a http.NewRequest. %s", err.Error())
	}

	// 设置query参数

	if req.QueryData != nil {
		req.PackQueryData(newReq)
	}

	// 设置header
	if req.Headers != nil {
		req.SetRequestHeader(newReq)
	}

	// 发起调用
	client := http.Client{Timeout: req.Timeout}
	resp, err := client.Do(newReq)
	if err != nil {
		return fmt.Errorf("failed to make request. %s", err.Error())
	}
	req.Response = resp
	req.StatusCode = resp.StatusCode

	if req.DoNotReadBody {
		return nil
	}
	return req.ReadBody()
}

// ------ 一些请求便捷方法 ------

func (req *Requests) Get() error {
	req.Method = "GET"
	return req.Request()
}

func (req *Requests) Head() error {
	req.Method = "HEAD"
	return req.Request()
}

func (req *Requests) Post() error {
	req.Method = "POST"
	return req.Request()
}

func (req *Requests) PUT() error {
	req.Method = "PUT"
	return req.Request()
}

func (req *Requests) Patch() error {
	req.Method = "PATCH"
	return req.Request()
}

//  ------ 结果处理方法------

// 读取Response.Body中的数据，将数据以string形式写入到Requests.Text属性中
// 一般会自动执行，除非手动指定Requests.DoNotReadBody = true
func (req *Requests) ReadBody() error {
	if strings.ToUpper(req.Method) == "HEAD" {
		req.isResponseBodyClosed = true
		return nil
	}
	if req.isResponseBodyClosed {
		return nil
	}
	defer req.Response.Body.Close()
	bytesData, err := ioutil.ReadAll(req.Response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body. " + err.Error())
	}
	req.Text = string(bytesData)
	req.isResponseBodyClosed = true
	return nil
}

// 一个便捷方法：自动执Requests.ReadBody()，然后返回Requests.Text的内容
func (req *Requests) GetText() (string, error) {
	if req.isResponseBodyClosed {
		return req.Text, nil
	}

	if err := req.ReadBody(); err != nil {
		return "", err
	}
	return req.Text, nil
}

// 一个便捷方法：自动执Requests.ReadBody()，然后尝试用json方式解析Requests.Text的内容到receiver
// 支持flag: 'NumberDecoder', 将使用tools.JSONDecode()来解析，对非struct的receiver有更好的兼容性。
func (req *Requests) Json(receiver interface{}, flags ...string) error {
	if !req.isResponseBodyClosed {
		if err := req.ReadBody(); err != nil {
			return err
		}
	}

	if tools.IsStrInSlice("NumberDecoder", flags) {
		if err := tools.JSONDecode(req.Text, receiver); err != nil {
			return fmt.Errorf("response data seems not a valid JSON. %s", err.Error())
		}
		return nil
	}

	// 默认使用原生的Decoder
	if err := json.Unmarshal([]byte(req.Text), receiver); err != nil {
		return fmt.Errorf("response data seems not a valid JSON. %s", err.Error())
	}
	return nil
}

// -------------------------------------- package级别的便捷函数 ------------------------------------

func Get(url string, headers map[string]string, queryData map[string]interface{}, timeout time.Duration) (*Requests, error) {
	req := Requests{
		Url:       url,
		Method:    "GET",
		Headers:   headers,
		QueryData: queryData,
		Timeout:   timeout,
	}
	err := req.Request()
	return &req, err
}

func Head(url string, headers map[string]string, queryData map[string]interface{}, timeout time.Duration) (*Requests, error) {
	req := Requests{
		Url:       url,
		Method:    "HEAD",
		Headers:   headers,
		QueryData: queryData,
		Timeout:   timeout,
	}

	err := req.Request()
	return &req, err
}

func Post(url string, headers map[string]string, data interface{}, queryData map[string]interface{}, timeout time.Duration) (*Requests, error) {
	req := Requests{
		Url:       url,
		Method:    "POST",
		Headers:   headers,
		Data:      data,
		QueryData: queryData,
		Timeout:   timeout,
	}

	err := req.Request()
	return &req, err
}

func Put(url string, headers map[string]string, data interface{}, queryData map[string]interface{}, timeout time.Duration) (*Requests, error) {
	req := Requests{
		Url:       url,
		Method:    "PUT",
		Headers:   headers,
		Data:      data,
		QueryData: queryData,
		Timeout:   timeout,
	}

	err := req.Request()
	return &req, err
}

func Patch(url string, headers map[string]string, data interface{}, queryData map[string]interface{}, timeout time.Duration) (*Requests, error) {
	req := Requests{
		Url:       url,
		Method:    "PATCH",
		Headers:   headers,
		Data:      data,
		QueryData: queryData,
		Timeout:   timeout,
	}

	err := req.Request()
	return &req, err
}

func Request(method string, url string, headers map[string]string, data interface{}, queryData map[string]interface{}, timeout time.Duration) (*Requests, error) {
	req := Requests{
		Url:       url,
		Method:    method,
		Headers:   headers,
		Data:      data,
		QueryData: queryData,
		Timeout:   timeout,
	}

	err := req.Request()
	return &req, err
}
