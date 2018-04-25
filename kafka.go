package main

import (
	"github.com/Shopify/sarama"
	"github.com/astaxie/beego/logs"
)

type LogData struct {
	line string
	topic string
}

type KafkaObj struct {
	consumer sarama.Consumer
	topic string
}

type KafkaMgr struct {
	topicMap map[string]*KafkaObj
	kafkaAddr string
	msgChan chan *LogData
}

var kafkaMgr *KafkaMgr

func initKafka(kafkaAddr string) (err error) {
	kafkaMgr = NewKafkaMgr(kafkaAddr, 100000)
	return
}

func NewKafkaMgr(kafkaAddr string, chanSize int) *KafkaMgr{
	km := &KafkaMgr{
		topicMap:make(map[string]*KafkaObj, 10),
		kafkaAddr: kafkaAddr,
		msgChan: make(chan *LogData, chanSize),
	}

	return km
}

func (k *KafkaMgr) AddTopic(topic string) {

	obj, ok := k.topicMap[topic]
	if ok {
		return
	}

	obj = &KafkaObj{
		topic: topic,
	}

	consumer, err := sarama.NewConsumer([]string{k.kafkaAddr}, nil)
	if err != nil {
		logs.Error("failed to connect kafka, err:%v", err)
		return
	}

	logs.Debug("connect to kafka succ, topic:%s", topic)
	obj.consumer = consumer
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		logs.Error("Failed to get the list of partitions, err:%v", err)
		return
	}

	for partition := range partitionList {
		pc, errRet := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if errRet != nil {
			err = errRet
			logs.Error("Failed to start consumer for partition %d: %s\n", partition, err)
			return
		}
		go func(p sarama.PartitionConsumer) {
			for msg := range p.Messages() {
				logs.Debug("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				logData := &LogData{
					line:string(msg.Value),
					topic: msg.Topic,
				}

				k.msgChan <- logData
			}
		}(pc)
	}
}

func  GetMessage() chan *LogData {
	return kafkaMgr.msgChan
}