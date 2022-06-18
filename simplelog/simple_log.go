package simplelog

/*
一个简单的日志处理工具

这里将日志分为 {
	"DEBUG": 0,
	"INFO": 1,
	"WARNING": 2,
	"ERROR": 3,
	"FATAL": 4,
	"PANIC": 5,
}
六个级别来分级触发，方便做全局设置。

注意：当前版本，仅能往stdout打印日志，不能打印到文件中。
*/

import (
	"log"
	"strings"
)

type SimpleLog struct {
	triggerLevel    string
	triggerLevelNum int

	MessagePrefix string
}

var level_map = map[string]int{
	"DEBUG":   0,
	"INFO":    1,
	"WARNING": 2,
	"ERROR":   3,
	"FATAL":   4,
	"PANIC":   5,
}

const DEFAULT_TRIGGER_LEVEL = "INFO"

var Slog *SimpleLog
var LogLevel string

// --------------------------------- logge方法 ---------------------------------

// The dispatcher.
func (logger *SimpleLog) dispatch(level string, message string) {
	if level_map[level] >= logger.triggerLevelNum {
		log.Printf("[%s] %s%s", level, logger.MessagePrefix, message)
	}
}

// The level methods.
func (logger *SimpleLog) Debug(message string) {
	logger.dispatch("DEBUG", message)
}

func (logger *SimpleLog) Info(message string) {
	logger.dispatch("INFO", message)
}

func (logger *SimpleLog) Warning(message string) {
	logger.dispatch("WARNING", message)
}

func (logger *SimpleLog) Warn(message string) {
	logger.dispatch("WARNING", message)
}

func (logger *SimpleLog) Error(message string) {
	logger.dispatch("ERROR", message)
}

func (logger *SimpleLog) Fatal(message string) {
	logger.dispatch("FATAL", message)
}

func (logger *SimpleLog) Panic(message string) {
	if level_map["PANIC"] >= logger.triggerLevelNum {
		log.Panicf("[PANIC] %s%s", logger.MessagePrefix, message)
	}
}

func (logger *SimpleLog) RemovePrefix() {
	logger.MessagePrefix = ""
}

// --------------------------------- logger初始化 ---------------------------------

func makeLogger(args ...string) *SimpleLog {
	logger := SimpleLog{
		triggerLevel:    DEFAULT_TRIGGER_LEVEL,
		triggerLevelNum: level_map[DEFAULT_TRIGGER_LEVEL],
	}

	// 设置logLevel.
	if len(args) >= 1 {
		triggerLevel := args[0]
		if triggerLevel == "" {
			triggerLevel = DEFAULT_TRIGGER_LEVEL
		}
		levelUpper := strings.ToUpper(triggerLevel)
		levelNum, ok := level_map[levelUpper]
		if !ok {
			panic("Illegal trigger level setting for `SimpleLog`!")
		}
		logger.triggerLevel = levelUpper
		logger.triggerLevelNum = levelNum
	}

	// 设置MessagePrefix.
	if len(args) >= 2 {
		prefix := args[1]
		logger.MessagePrefix = prefix
	}

	return &logger
}

// 初始化一个全局logger: Slog
func SlogInit(args ...string) {
	Slog = makeLogger(args...)
}

// 构造并返回一个新的logger
func SlogNew(args ...string) *SimpleLog {
	logger := makeLogger(args...)
	return logger
}

// ---------------------------------- 快捷方法 ------------------------------------

func dispatch(level string, message string) {
	if Slog == nil {
		SlogInit()
	}
	if level == "PANIC" {
		Slog.Panic(message)
	} else {
		Slog.dispatch(level, message)
	}
}

func Debug(message string) {
	dispatch("DEBUG", message)
}

func Info(message string) {
	dispatch("INFO", message)
}

func Warning(message string) {
	dispatch("WARNING", message)
}

func Warn(message string) {
	dispatch("WARNING", message)
}

func Error(message string) {
	dispatch("ERROR", message)
}

func Fatal(message string) {
	dispatch("FATAL", message)
}

func Panic(message string) {
	dispatch("PANIC", message)
}

func SetPrefix(prefix string) {
	if Slog == nil {
		SlogInit()
	}
	Slog.MessagePrefix = prefix
}

func RemovePrefix() {
	if Slog == nil {
		return
	}
	Slog.MessagePrefix = ""
}
