package test

import (
	"fmt"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/db"
	"github.com/ketianlin/kgin/logs"
	"testing"
)

func TestMysql(t *testing.T) {
	//初始化配置
	configFile := "/home/ke666/my_codes/go_codes/kgin/test/kgin.yml"
	fmt.Println(configFile)
	kgin.Init(configFile)
	addUser()
	//getUsers()
	select {}
}

func addUser() {
	conn, err := db.Mysql.GetConnection()
	if err != nil {
		logs.Error("连接失败:{}", err)
		return
	}
	for i := 0; i < 5; i++ {
		u := &User{
			Name: fmt.Sprintf("吊毛-%d", i),
			Age:  99,
		}
		err = conn.Create(u).Error
		if err != nil {
			logs.Error("保存数据失败:{}", err)
			return
		}
	}
	logs.Info("新增用户success")
}

func getUsers() {
	conn, err := db.Mysql.GetConnection()
	if err != nil {
		logs.Error("连接失败:{}", err)
		return
	}
	userList := make([]User, 0)
	err = conn.Model(new(User)).Where("id>2").Find(&userList).Error
	if err != nil {
		logs.Error("查询失败:{}", err)
		return
	}
	for _, user := range userList {
		logs.Info("{}", user)
	}
	logs.Info("------------")
}
