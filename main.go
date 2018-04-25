package main

import (
	"fmt"
	"github.com/astaxie/beego/logs"
	"github.com/pythonsite/config"
	"encoding/json"
	"time"
)

type AppConfig struct {
	logPath string
	logLevel string
	kafkaAddr string
	esAddr string
	esThreadNum int
	etcdAddr string
	etcdKeyFormat string
}

var appConfig AppConfig

func initConfig(confPath string) (err error) {
	// 初始化配置信息
	conf, err := config.NewConfig(confPath)
	if err != nil {
		return
	}

	logPath, err := conf.GetString("log_path")
	if len(logPath) == 0 || err != nil{
		return fmt.Errorf("get log_path failed,invalid logPath, err:%v", err)
	}

	appConfig.logPath = logPath
	logLevel, err := conf.GetString("log_level")
	if len(logLevel) == 0 || err != nil {
		return fmt.Errorf("get logLevel failed,invalid logLevel, err:%v", err)
	}

	appConfig.logLevel = logLevel

	kafkaAddr, err := conf.GetString("kafka_addr")
	if len(kafkaAddr) == 0 || err != nil {
		return fmt.Errorf("get kafkaAddr failed,invalid kafkaAddr, err:%v", err)
	}

	appConfig.kafkaAddr = kafkaAddr

	esAddr, err := conf.GetString("es_addr")
	if len(kafkaAddr) == 0 || err != nil {
		return fmt.Errorf("get es_addr failed,invalid es_addr, err:%v", err)
	}

	appConfig.esAddr = esAddr

	esThreadNum := conf.GetIntDefault("es_thread_num", 8)
	appConfig.esThreadNum = esThreadNum


	etcdAddr, err := conf.GetString("etcd_addr")
	if len(etcdAddr) == 0 || err != nil {
		return fmt.Errorf("get etcdAddr failed,invalid etcdAddr, err:%v", err)
	}

	appConfig.etcdAddr = etcdAddr

	etcdKey, err := conf.GetString("etcd_transfer_key")
	if len(etcdAddr) == 0 || err != nil {
		return fmt.Errorf("get etcd_transfer_key failed,invalid etcd_transfer_key, err:%v", err)
	}
	appConfig.etcdKeyFormat = etcdKey

	return
}

func getLevel(level string) int {
	switch level {
	case "debug":
		return logs.LevelDebug
	case "trace":
		return logs.LevelTrace
	case "warn":
		return logs.LevelWarning
	case "info":
		return logs.LevelInformational
	case "error":
		return logs.LevelError
	default:
		return logs.LevelDebug
	}
}

func initLog(logPath string, logLevel string) (err error) {
	// 初始化日志配置
	config := make(map[string]interface{})
	config["filename"] = logPath
	config["level"] = getLevel(logLevel)
	configStr, err := json.Marshal(config)
	if err != nil {
		fmt.Println("marshal failed, err:", err)
		return
	}
	logs.SetLogger(logs.AdapterFile, string(configStr))
	return
}


func main(){
	err := initConfig("./conf/app.conf")
	if err != nil {
		panic(fmt.Sprintf("init config failed, err:%v", err))
	}

	err = initLog(appConfig.logPath, appConfig.logLevel)
	if err != nil {
		panic(fmt.Sprintf("init log failed, err:%v", err))
	}

	logs.Debug("init log succ, config:%#v", appConfig)

	err = initKafka(appConfig.kafkaAddr)
	if err != nil {
		logs.Error("init kafka failed, err:%v", err)
		return
	}

	err = initEs(appConfig.esAddr)
	if err!= nil {
		logs.Error("init es failed, err:%v", err)
		return
	}

	ips, err := getLocalIP();
	if err != nil {
		logs.Error("get local ip failed, er:%v", err)
		return
	}

	logs.Debug("etcd addr:%s", appConfig.etcdAddr)
	err = initEtcd([]string{appConfig.etcdAddr}, appConfig.etcdKeyFormat, ips, 5*time.Second)
	if err != nil {
		logs.Error("initEtcd failed, er:%v", err)
		return
	}

	err = Run(appConfig.esThreadNum)
	if err != nil {
		logs.Error("run es failed, err:%v", err)
		return
	}
	logs.Debug("run exited")
}
