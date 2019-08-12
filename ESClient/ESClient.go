package ESClient

import (
	"context"
	"errors"
	"fmt"
	"github.com/luciferofwg/log"
	"github.com/olivere/elastic"
	"sync"
	"time"
)

type ESClient struct {
	ch     chan interface{}
	client *elastic.Client
	bulk   *elastic.BulkProcessor
	wg     sync.WaitGroup
	logger *log.MyLog
	host   string
	bInit  bool
	bExit  bool
}
type ESParam struct {
	Index string      //索引
	Typ   string      //类型
	Value interface{} //数据
}

func NewES(host string) *ESClient {
	return &ESClient{
		ch:     make(chan interface{}, 10),
		client: nil,
		bulk:   nil,
		logger: nil,
		host:   host,
		bInit:  false,
		bExit:  false,
	}
}

func (this *ESClient) Init() error {
	addr := "http://" + this.host
	if this.bInit {
		return errors.New("已经初始化")
	}
	var err error
	curName := GetProecessName()
	if err != nil {
		return errors.New("获取可执行程序路径失败，错误：%v" + err.Error())
	}
	file := curName + "/ESClient/default.log"
	this.logger = log.NewLog()
	if !this.logger.Init(file) {
		return errors.New("初始化日志模块失败")
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

	cfg := []elastic.ClientOptionFunc{
		elastic.SetURL(addr),
		elastic.SetSniff(true),
		elastic.SetInfoLog(*logInfo),
		elastic.SetTraceLog(*logTrace),
		elastic.SetErrorLog(*logError),
		elastic.SetSnifferTimeoutStartup(time.Second * 2), //初始创建客户端时的超时时间
		elastic.SetSnifferTimeout(time.Second * 2),        //初始创建客户端时的超时时间
		elastic.SetSnifferInterval(time.Second * 60),
	}
	this.client, err = elastic.NewClient(cfg...)
	if err != nil {
		str := fmt.Sprintf("创建elastic client失败 host=%s，错误：%v", addr, err)
		return errors.New(str)
	}

	version, err := this.client.ElasticsearchVersion(addr)
	if err != nil {
		this.logger.Error("获取Elasticsearch版本失败，错误：%v", err)
		return err
	}
	this.logger.Debug("Elasticsearch 的版本是：%s", version)

	//创建一个 BulkProcessor
	this.bulk, err = this.client.BulkProcessor().
		Name("MyBackgroundWorker-1").
		Before(this.beforeCallback).    // func to call before commits
		After(this.afterCallback).      // func to call after commits
		Workers(2).                     // number of workers
		BulkActions(100).               // commit if # requests >= 1000
		BulkSize(-1).                   // commit if size of requests >= 2 MB
		FlushInterval(2 * time.Second). // commit every 30s
		Stats(true).
		Do(context.Background())
	if err != nil {
		str := fmt.Sprintf("创建BulkProcessor失败,错误：%v", err)
		return errors.New(str)
	}
	this.logger.Info("ES客户端初始化完成")
	this.bInit = true

	//发送数据的线程
	this.wg.Add(1)
	go this.run("线程")

	return nil
}

func (this *ESClient) Push(data interface{}) error {
	if !this.bInit {
		return errors.New("未初始化客户端")
	}
	this.ch <- data
	return nil
}

func (this *ESClient) run(strName string) {
	defer this.wg.Done()
	defer func() {
		this.logger.Info("bulk数据发送 %s 退出", strName)
	}()
	this.logger.Info("bulk数据发送 %s 启动", strName)
	for {
		select {
		case data, _ := <-this.ch:
			index := data.(ESParam).Index
			typ := data.(ESParam).Typ
			value := data.(ESParam).Value
			this.logger.Debug("开始申请BulkIndexRequest")
			uuid := GenetateUUID()
			r := elastic.NewBulkIndexRequest().Index(index).Type(typ).Id(uuid).Doc(value)
			this.bulk.Add(r)
			this.logger.Debug("当前UUID值：%s", uuid)
		default:
			if this.bExit {
				return
			}
		}
		time.Sleep(time.Microsecond * 1)
	}
}
func (this *ESClient) Fini() error {
	this.logger.Debug("ES客户端开始退出")
	this.bExit = true

	this.wg.Wait()
	return this.bulk.Close()
}

func (this *ESClient) beforeCallback(executionId int64, requests []elastic.BulkableRequest) {
	//fmt.Printf("executionId=%v\n", executionId)
}

func (this *ESClient) afterCallback(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	defer func() {
		if err := recover(); err != nil {
			this.logger.Critical("提交数据错误处理时异常，错误：%v", err)
		}
	}()
	if err != nil {
		this.logger.Error("提交数据失败，executionId=%d,错误：%v", executionId, err)
		this.logger.Error("错误数据：%+v", response)
	}
	if response != nil && response.Errors {
		for _, mapBrespItem := range response.Items {
			item := *(mapBrespItem["index"])

			index := item.Index
			typ := item.Type
			id := item.Id
			status := item.Status
			errDetails := item.Error
			str := fmt.Sprintf("提交数据失败，基础信息：\nid：%v， 索引：%v， 类型：%v，status：%v， 失败原因：%v", id, index, typ, status, errDetails.Reason)
			this.logger.Error(str)
		}
	}
	this.logger.Info("###############################")
}
