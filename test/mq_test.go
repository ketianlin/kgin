package test

import (
	"encoding/json"
	"fmt"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/db/rabbitmq"
	"github.com/ketianlin/kgin/logs"
	"runtime/debug"
	"testing"
)

const RouterKey = "diao_mao"

type User struct {
	Id   int64  `json:"id" gorm:"id"`
	Name string `json:"name" gorm:"name"`
	Age  int    `json:"age" gorm:"age"`
}

func (User) TableName() string {
	return "user"
}

type testEventHandler struct{}

var Test testEventHandler

func (o *testEventHandler) handleExpireMsg(msg []byte) {
	logs.Debug("接收到原始消息 {}", string(msg))
	safeHandler(msg, func(msg []byte) {
		var u User
		err := json.Unmarshal(msg, &u)
		if err != nil {
			logs.Error("json反序列化失败 {}", err.Error())
			return
		}
		logs.Info("打印序列化后消息：{}", u)
	})
}

func safeHandler(msg []byte, handler func(msg []byte)) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logs.Error("goroutine process panic {} stack: {}", err, string(debug.Stack()))
			}
		}()
		handler(msg)
	}()
}

func TestMq(t *testing.T) {
	//初始化配置
	configFile := "/home/ke666/my_codes/go_codes/kgin/test/kgin.yml"
	fmt.Println(configFile)
	kgin.Init(configFile)
	kgin.KGin.Use("rabbitmq", rabbitmq.Rabbit.Init, rabbitmq.Rabbit.Close, nil)
	ListenRMQ()
	SendMessage()
	select {}
}

func ListenRMQ() {
	conn := rabbitmq.Rabbit.GetConnection()
	logs.Debug("rabbitmq 连接获取成功")
	conn.RabbitMessageListener(RouterKey, Test.handleExpireMsg)
}

func SendMessage() {
	logs.Info("开始发送...")
	for i := 0; i < 3; i++ {
		conn := rabbitmq.Rabbit.GetConnection()
		id := int64(i + 666)
		u := &User{
			Id:   id,
			Name: fmt.Sprintf("阿三-%d", id),
			Age:  98,
		}
		msg, _ := json.Marshal(u)
		conn.RabbitSendMessage(RouterKey, string(msg))
		//time.Sleep(time.Second * 1)
	}
	logs.Info("发送完毕,successfully")
}
