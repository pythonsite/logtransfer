package main

import (
	"context"
	"github.com/astaxie/beego/logs"
	"github.com/coreos/etcd/clientv3"
	"time"
	"fmt"
)

var etcdClient *clientv3.Client
var logConfChan chan string

func initEtcd(addr []string, keyfmt string, ipArrays []string, timeout time.Duration) (err error) {

	var keys []string
	for _, ip := range ipArrays {
		keys = append(keys, fmt.Sprintf(keyfmt, ip))
	}

	logConfChan = make(chan string, 8)
	logs.Debug("etcd watch key:%v, timeout:%d", keys, timeout)

	etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   addr,
		DialTimeout: timeout,
	})
	if err != nil {
		logs.Warn("init etcd client failed, err:%v", err)
		return
	}

	logs.Debug("init etcd succ")
	waitGroup.Add(1)

	for  _, key := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), 2 *time.Second)
		///logagent/192.168.2.100/log_config
		resp, err := etcdClient.Get(ctx, key)
		cancel()
		if err != nil {
			logs.Warn("get key %s failed, err:%v", key, err)
			continue
		}

		for _, ev := range resp.Kvs {
			logs.Debug(" %q : %q\n",  ev.Key, ev.Value)
			logConfChan <- string(ev.Value)
		}
	}
	go WatchEtcd(keys)
	return
}

func WatchEtcd(keys []string) {
	var watchChans []clientv3.WatchChan
	for _, key := range keys {
		rch := etcdClient.Watch(context.Background(), key)
		watchChans = append(watchChans, rch)
	}

	for {
		for _, watchC := range watchChans {
			select {
			case wresp := <- watchC:
				for _, ev := range wresp.Events {
					logs.Debug("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
					logConfChan <- string(ev.Kv.Value)
				}
			default:
			}
		}

		time.Sleep(time.Second)
	}

	waitGroup.Done()
}

func GetLogConf() chan string {
	return logConfChan
}