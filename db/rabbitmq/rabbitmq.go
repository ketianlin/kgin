package rabbitmq

import (
	"github.com/ketianlin/kgin/jazz"
	"github.com/ketianlin/kgin/logs"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"io/ioutil"
	"strings"
)

type rabbitmq struct {
	conf       *koanf.Koanf
	confUrl    string
	connection *connection // 连接
}

type connection struct {
	conn     *jazz.Connection
	exchange string
	dsn      string
}

var Rabbit = &rabbitmq{}

func (r *rabbitmq) Init(rabbitConfigUrl string) {
	if rabbitConfigUrl != "" {
		r.confUrl = rabbitConfigUrl
	}
	if r.confUrl == "" {
		logs.Error("rabbit配置Url为空")
		return
	}
	if r.connection == nil {
		if r.conf == nil {
			var confData []byte
			var err error
			if strings.HasPrefix(r.confUrl, "http://") {
				resp, err := grequests.Get(r.confUrl, nil)
				if err != nil {
					logs.Error("MySQL配置下载失败!{} ", err.Error())
					return
				}
				confData = []byte(resp.String())
			} else {
				confData, err = ioutil.ReadFile(r.confUrl)
				if err != nil {
					logs.Error("MySQL本地配置文件{}读取失败:{}", r.confUrl, err.Error())
					return
				}
			}
			r.conf = koanf.New(".")
			err = r.conf.Load(rawbytes.Provider(confData), yaml.Parser())
			if err != nil {
				logs.Error("MongoDB配置解析错误:{}", err.Error())
				r.conf = nil
				return
			}
		}
		dsn := r.conf.String("go.rabbitmq.uri")
		conn, err := jazz.Connect(dsn)
		if err != nil {
			logs.Error("RabbitMQ连接错误:{}", err.Error())
		} else {
			r.connection = &connection{
				conn:     conn,
				exchange: r.conf.String("go.rabbitmq.exchange"),
				dsn:      dsn,
			}
		}
	}
}

func (r *rabbitmq) Close() {
	r.connection.conn.Close()
}

// GetConnection 获取连接
func (r *rabbitmq) GetConnection() *connection {
	return r.connection
}

// RabbitSendMessage 向指定队列发送消息
func (r *connection) RabbitSendMessage(queueName string, msg string) {
	err := r.conn.SendMessage(r.exchange, queueName, msg)
	if err != nil {
		logs.Error("RabbitMQ发送消息错误:{}", err.Error())
	}
}

// RabbitMessageListener 侦听指定队列消息，内部自建侦听协程
func (r *connection) RabbitMessageListener(queueName string, listener func(msg []byte)) {
	//侦听之前先创建队列
	r.RabbitCreateNewQueue(queueName)
	//启动侦听消息处理线程
	go r.conn.ProcessQueue(queueName, listener)
}

// RabbitCreateNewQueue 创建队列
func (r *connection) RabbitCreateNewQueue(queueName string) {
	queues := make(map[string]jazz.QueueSpec)
	binding := &jazz.Binding{
		Exchange: r.exchange,
		Key:      queueName,
	}
	queueSpec := &jazz.QueueSpec{
		Durable:  true,
		Bindings: []jazz.Binding{*binding},
		Args:     nil,
	}
	queues[queueName] = *queueSpec
	setting := &jazz.Settings{
		Queues: queues,
	}
	err := r.conn.CreateScheme(*setting)
	if err != nil {
		logs.Error("RabbitMQ创建队列失败:{}", err.Error())
	}
}

// RabbitCreateDeadLetterQueue 创建死信队列
func (r *connection) RabbitCreateDeadLetterQueue(queueName, toQueueName string, ttl int) {
	queues := make(map[string]jazz.QueueSpec)
	binding := &jazz.Binding{
		Exchange: r.exchange,
		Key:      queueName,
	}
	queueSpec := &jazz.QueueSpec{
		Durable:  true,
		Bindings: []jazz.Binding{*binding},
		Args:     jazz.DeadLetterArgs(ttl, r.exchange, toQueueName),
	}
	queues[queueName] = *queueSpec
	setting := &jazz.Settings{
		Queues: queues,
	}
	err := r.conn.CreateScheme(*setting)
	if err != nil {
		logs.Error("RabbitMQ创建死信队列失败:" + err.Error())
	}
}
