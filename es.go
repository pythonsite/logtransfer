package main

import (
	"gopkg.in/olivere/elastic.v2"
	"github.com/astaxie/beego/logs"
	"sync"
	"encoding/json"
)

var waitGroup sync.WaitGroup

var client *elastic.Client

func initEs(addr string,) (err error){
	client,err = elastic.NewClient(elastic.SetSniff(false),elastic.SetURL(addr))
	if err != nil{
		logs.Error("connect to es error:%v",err)
		return
	}
	logs.Debug("conn to es success")
	return
}

func reloadKafka(topicArray []string) {
	for _, topic := range topicArray{
		kafkaMgr.AddTopic(topic)
	}
}

func reload(){
	for conf := range GetLogConf(){
		var topicArray []string
		err := json.Unmarshal([]byte(conf),&topicArray)
		if err != nil {
			logs.Error("unmarshal failed,err:%v conf:%v",err,conf)
			continue
		}
		reloadKafka(topicArray)
	}
}

func Run(esThreadNum int) (err error) {

	go reload()

	for i:=0;i<esThreadNum;i++{
		waitGroup.Add(1)
		go sendToEs()
	}
	waitGroup.Wait()
	return
}

type EsMessage struct {
	Message string
}

func sendToEs(){
	for msg:= range GetMessage() {
		var esMsg EsMessage
		esMsg.Message = msg.line
		_,err := client.Index().Index(msg.topic).Type(msg.topic).BodyJson(esMsg).Do()
		if err != nil {
			logs.Error("send to es failed,err:%v",err)
			continue
		}
		logs.Debug("send to es success")
	}
	waitGroup.Done()
}


