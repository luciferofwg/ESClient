package main

import (
	"errors"
	"fmt"
	"github.com/luciferofwg/ESClient/global"
	"github.com/valyala/fasthttp"
	"net/http"
	"time"
)

func CreateIndexModels(host, modelName, text string) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetContentType("application/json")
	req.Header.SetMethod(http.MethodPost)
	url := fmt.Sprintf("http://%s:9200/_template/template_%s", host, modelName)
	req.Header.SetHost(url)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.DoTimeout(req, resp, time.Millisecond*500); err != nil && resp.StatusCode() != http.StatusOK {
		return errors.New(fmt.Sprintf("发送模板创建命令时失败，超时时间：%v ms， 错误：%v", time.Millisecond*500, err))
	}

	// 查询模板
	if err := QueryIndexModels(host, modelName); err != nil {
		return err
	}
	return nil
}

func QueryIndexModels(host, modelName string) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(http.MethodGet)
	url := fmt.Sprintf("http://%s:9200/_template/template_%s", host, modelName)
	req.Header.SetHost(url)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.DoTimeout(req, resp, time.Millisecond*500); err != nil && resp.StatusCode() != http.StatusOK {
		return errors.New(fmt.Sprintf("发送模板查询命令时失败，超时时间：%v ms， 错误：%v", time.Millisecond*500, err))
	} else {
		global.Log.Info("查询到的模板为：%v", string(resp.Body()))
	}
	return nil
}
