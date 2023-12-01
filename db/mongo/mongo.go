package mongo

import (
	"context"
	"errors"
	"fmt"
	"github.com/ketianlin/kgin/logs"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/qiniu/qmgo"
	"io/ioutil"
	"strings"
	"time"
)

type MongoDBConf struct {
	Uri      string
	Username string
	Password string
	DB       string
	Timeout  int64
	Max      uint64
	Min      uint64
}

type MongoDB struct {
	conf        *koanf.Koanf
	confUrl     string
	conn        *qmgo.Database
	client      *qmgo.Client
	mongoDBConf *MongoDBConf // 保存配置，方便创建事务的连接客户端
}

func (m *MongoDB) getQMgoConfig(collNames ...string) *qmgo.Config {
	conf := &qmgo.Config{
		Uri:             m.mongoDBConf.Uri,
		MaxPoolSize:     &m.mongoDBConf.Max,
		MinPoolSize:     &m.mongoDBConf.Min,
		SocketTimeoutMS: &m.mongoDBConf.Timeout,
		Auth: &qmgo.Credential{
			Username: m.mongoDBConf.Username,
			Password: m.mongoDBConf.Password,
		},
	}
	if len(collNames) > 0 {
		conf.Database = m.mongoDBConf.DB // 数据库
		conf.Coll = collNames[0]         // 集合
	}
	return conf
}

func (m *MongoDB) Init(mongodbConfigUrl string) {
	if mongodbConfigUrl != "" {
		m.confUrl = mongodbConfigUrl
	}
	if m.confUrl == "" {
		logs.Error("MongoDB配置Url为空")
		return
	}
	if m.conn == nil {
		var confData []byte
		var err error
		if strings.HasPrefix(m.confUrl, "http://") {
			resp, err := grequests.Get(m.confUrl, nil)
			if err != nil {
				logs.Error("MongoDB配置下载失败! " + err.Error())
				return
			}
			confData = []byte(resp.String())
		} else {
			confData, err = ioutil.ReadFile(m.confUrl)
			if err != nil {
				logs.Error(fmt.Sprintf("MongoDB本地配置文件%s读取失败:%s", m.confUrl, err.Error()))
				return
			}
		}
		m.conf = koanf.New(".")
		err = m.conf.Load(rawbytes.Provider(confData), yaml.Parser())
		if err != nil {
			logs.Error("MongoDB配置文件解析错误:" + err.Error())
			m.conf = nil
			return
		}
		m.mongoDBConf = &MongoDBConf{
			Uri:      m.conf.String("go.data.mongodb.uri"),
			Username: m.conf.String("go.data.mongodb.username"),
			Password: m.conf.String("go.data.mongodb.password"),
			DB:       m.conf.String("go.data.mongodb.db"),
			Timeout:  m.conf.Int64("go.data.mongodb.timeout"),
			Max:      uint64(m.conf.Int("go.data.mongo_pool.max")),
			Min:      uint64(m.conf.Int("go.data.mongo_pool.min")),
		}
		//uri := m.conf.String("go.data.mongodb.uri")
		//username := m.conf.String("go.data.mongodb.username")
		//password := m.conf.String("go.data.mongodb.password")
		//db := m.conf.String("go.data.mongodb.db")
		//timeout := m.conf.Int64("go.data.mongodb.timeout")
		//max := uint64(m.conf.Int("go.data.mongo_pool.max"))
		//min := uint64(m.conf.Int("go.data.mongo_pool.min"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.mongoDBConf.Timeout)*time.Second)
		defer cancel()
		qConf := m.getQMgoConfig()
		client, err := qmgo.NewClient(ctx, qConf)
		if err != nil {
			logs.Error("MongoDB连接错误:{}", err.Error())
			return
		}
		err = client.Ping(m.mongoDBConf.Timeout)
		if err != nil {
			logs.Error("MongoDB连接错误:{}", err.Error())
			return
		}
		m.client = client
		m.conn = client.Database(m.mongoDBConf.DB)
		m.mongoDBConf = &MongoDBConf{
			m.mongoDBConf.Uri,
			m.mongoDBConf.Username,
			m.mongoDBConf.Password,
			m.mongoDBConf.DB,
			m.mongoDBConf.Timeout,
			m.mongoDBConf.Max,
			m.mongoDBConf.Min,
		}
	}
}

func (m *MongoDB) Close() {
	if m.client != nil {
		err := m.client.Close(context.Background())
		if err != nil {
			logs.Error("MongoDB关闭连接错误:{}", err.Error())
			return
		}
	}
	m.conn = nil
	m.client = nil
}

func (m *MongoDB) GetConnection() *qmgo.Database {
	return m.conn
}

func (m *MongoDB) Check() error {
	_, err := mongoCheck(m)
	if err != nil {
		logs.Error(err.Error())
	}
	return err
}

func mongoCheck(m *MongoDB) (*qmgo.Database, error) {
	if m.conn == nil {
		m.Init("")
		if m.conn == nil {
			return nil, errors.New("mongodb connection error")
		}
	}
	return m.conn, nil
}

// GetTransactionClient 这个方法肯定是在初始化后才有可能被调用
func (m *MongoDB) GetTransactionClient(collName string) (*qmgo.QmgoClient, error) {
	if m.mongoDBConf == nil {
		return nil, errors.New("配置文件不存在")
	}
	//poolMonitor := &event.PoolMonitor{
	//	Event: func(evt *event.PoolEvent) {
	//		switch evt.Type {
	//		case event.GetSucceeded:
	//			fmt.Println("GetSucceeded")
	//		case event.ConnectionReturned:
	//			fmt.Println("ConnectionReturned")
	//		}
	//	},
	//}
	//
	//opt := options.Client().SetPoolMonitor(poolMonitor) // more options use the chain options.
	qConf := m.getQMgoConfig(collName)
	cli, err := qmgo.Open(context.Background(), qConf)
	//cli, err := Open(ctx, &Config{Uri: URI, Database: DATABASE, Coll: COLL}, opt)
	if err != nil {
		return nil, err
	}
	return cli, nil
}
