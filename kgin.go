package kgin

import (
	"github.com/ketianlin/kgin/cache"
	"github.com/ketianlin/kgin/config"
	"github.com/ketianlin/kgin/db"
	"github.com/ketianlin/kgin/logs"
	"strings"
	"time"
)

type kgin struct {
	plugins map[string]plugin
}

type plugin struct {
	InitFunc  dbInitFunc
	CloseFunc dbCloseFunc
	CheckFunc dbCheckFunc
}

type dbInitFunc func(configUrl string)
type dbCloseFunc func()
type dbCheckFunc func() error

var KGin = &kgin{}

func (m *kgin) Use(dbConfigName string, dbInit dbInitFunc, dbClose dbCloseFunc, dbCheck dbCheckFunc) {
	if !strings.Contains(config.Config.Config.Used, dbConfigName) {
		logs.Error("加载{}失败，配置文件中未使用", dbConfigName)
		return
	}
	cnfUrl := config.Config.GetConfigUrl(config.Config.GetConfigString("go.config.prefix." + dbConfigName))
	if cnfUrl == "" {
		logs.Error("{}配置错误，无法获取配置地址", dbConfigName)
		return
	}
	if m.plugins == nil {
		m.plugins = make(map[string]plugin)
	}
	m.plugins[dbConfigName] = plugin{
		InitFunc:  dbInit,
		CloseFunc: dbClose,
		CheckFunc: dbCheck,
	}
	logs.Info("正在连接{}", dbConfigName)
	dbInit(cnfUrl)
	logs.Info("{}连接成功", dbConfigName)
}

func Init(configFile string) {
	config.Config.Init(configFile)
	configs := config.Config.Config.Used

	if strings.Contains(configs, "mysql") {
		logs.Info("正在连接MySQL")
		db.Mysql.Init(config.Config.GetConfigUrl(config.Config.Config.Prefix.Mysql))
		logs.Info("连接MySQL成功")
	}
	if strings.Contains(configs, "redis") {
		logs.Info("正在连接Redis")
		db.Redis.Init(config.Config.GetConfigUrl(config.Config.Config.Prefix.Redis))
		logs.Info("连接Redis成功")
	}
	if strings.Contains(configs, "mongodb") {
		logs.Info("正在连接MongoDB")
		db.Mongo.Init(config.Config.GetConfigUrl(config.Config.Config.Prefix.Mongodb))
		logs.Info("连接MongoDB成功")
	}
	if strings.Contains(configs, "sqlite") {
		logs.Info("正在连接SQLite")
		db.Sqlite.Init(config.Config.Config.Prefix.Sqlite)
		logs.Info("连接SQLite成功")
	}

	//设置定时任务自动检查
	ticker := time.NewTicker(time.Minute * 5)
	go func() {
		for _ = range ticker.C {
			KGin.checkAll()
		}
	}()
	return
}

func (m *kgin) checkAll() {
	configs := config.Config.Config.Used
	var err error
	if strings.Contains(configs, "mysql") {
		logs.Info("正在检查MySQL")
		err = db.Mysql.Check()
		if err != nil {
			logs.Error("MySQL check failed： {}", err.Error())
		}
	}
	if strings.Contains(configs, "redis") {
		logs.Info("正在检查Redis")
		err = db.Redis.Check()
		if err != nil {
			logs.Error("Redis check failed： {}", err.Error())
		}
	}
	if strings.Contains(configs, "mongodb") {
		logs.Info("正在检查MongoDB")
		err = db.Mongo.Check()
		if err != nil {
			logs.Error("MongoDB check failed： {}", err.Error())
		}
	}
	if m.plugins != nil {
		for dbConfigName, pl := range m.plugins {
			if pl.CheckFunc != nil {
				logs.Info("正在检查{}", dbConfigName)
				err := pl.CheckFunc()
				if err != nil {
					logs.Error("{}连接检查失败:{}", dbConfigName, err.Error())
				}
			}
		}
	}
}

func (m *kgin) SafeExit() {
	configs := config.Config.Config.Used
	if strings.Contains(configs, "mysql") {
		logs.Info("正在关闭MySQL连接")
		db.Mysql.Close()
	}
	if strings.Contains(configs, "redis") {
		logs.Info("正在关闭Redis连接")
		db.Redis.Close()
	}
	if strings.Contains(configs, "sqlite") {
		logs.Info("正在关闭SQLite连接")
		db.Sqlite.Close()
	}
	if strings.Contains(configs, "mongodb") {
		logs.Info("正在关闭MongoDB连接")
		db.Mongo.Close()
	}
	if m.plugins != nil {
		for dbConfigName, pl := range m.plugins {
			if pl.CloseFunc != nil {
				logs.Info("正在关闭{}", dbConfigName)
				pl.CloseFunc()
			}
		}
	}
	cache.CloseCache()
}
