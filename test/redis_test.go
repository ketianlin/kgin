package test

import (
	"fmt"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/db"
	"github.com/ketianlin/kgin/logs"
	"testing"
	"time"
)

func TestRedis(t *testing.T) {
	//初始化配置
	configFile := "/home/ke666/my_codes/go_codes/kgin/test/kgin.yml"
	fmt.Println(configFile)
	kgin.Init(configFile)
	setData()
	getData()
	select {}
}

func getData() {
	conn := db.Redis.GetConnection()
	result, err := conn.Get("flag").Result()
	if err != nil {
		logs.Error("获取数据异常：{}", err)
		return
	}
	logs.Info("{}", result)
}

func setData() {
	conn := db.Redis.GetConnection()
	err := conn.Set("flag", "吊毛001", 50*time.Second).Err()
	fmt.Println(err)
}
