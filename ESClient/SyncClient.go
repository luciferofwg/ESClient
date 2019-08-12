package ESClient

import (
	"errors"
	"fmt"
	"github.com/luciferofwg/log"
	"github.com/olivere/elastic"
)

type SyncClient struct {
	client *elastic.Client
	host   string
	bInit  bool
}

func NewSyncClient() *SyncClient {
	return &SyncClient{}
}

func (this *SyncClient) Init() error {
	addr := "http://" + this.host
	if this.bInit {
		return errors.New("已经初始化")
	}
	var err error
	curName := GetProecessName()
	if err != nil {
		return errors.New("获取可执行程序路径失败，错误：%v" + err.Error())
	}

	file1 := curName + "/ESClient/info.log"
	logInfo := log.NewLog()
	if !logInfo.Init(file1) {
		return errors.New("初始化日志模块失败")
	}
	file2 := curName + "/ESClient/trace.log"
	logTrace := log.NewLog()
	if !logTrace.Init(file2) {
		return errors.New("初始化日志模块失败")
	}
	file3 := curName + "/ESClient/error.log"
	logError := log.NewLog()
	if !logError.Init(file3) {
		return errors.New("初始化日志模块失败")
	}
	this.client, err = elastic.NewClient(
		elastic.SetInfoLog(logInfo),
		elastic.SetTraceLog(logTrace),
		elastic.SetErrorLog(logError),
		elastic.SetURL(addr),
		elastic.SetSniff(true),
	)
	if err != nil {
		str := fmt.Sprintf("创建elastic client失败 host=%s，错误：%v", addr, err)
		return errors.New(str)
	}
	this.bInit = true
	return nil
}

func (this *SyncClient) Insert() error {
	return nil
}
