package global

import (
	"github.com/luciferofwg/log"
)

var Log *log.MyLog

func init() {
	Log = log.NewLog()
	if !Log.Init("/Demo/default.log") {
		panic("日志初始化失败")
	}
}
