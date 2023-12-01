package mysql

import (
	"errors"
	"fmt"
	"github.com/ketianlin/kgin/logs"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io/ioutil"
	"strings"
	"time"
)

type MysqlClient struct {
	mysql   *gorm.DB
	conf    *koanf.Koanf
	confUrl string
}

func (m *MysqlClient) Init(mysqlConfigUrl string) {
	if mysqlConfigUrl != "" {
		m.confUrl = mysqlConfigUrl
	}
	if m.confUrl == "" {
		logs.Error("MySQL配置文件Url为空")
		return
	}
	if m.mysql == nil {
		var confData []byte
		var err error
		if strings.HasPrefix(m.confUrl, "http://") {
			resp, err := grequests.Get(m.confUrl, nil)
			if err != nil {
				logs.Error("MySQL配置下载失败! " + err.Error())
				return
			}
			confData = []byte(resp.String())
		} else {
			confData, err = ioutil.ReadFile(m.confUrl)
			if err != nil {
				logs.Error(fmt.Sprintf("MySQL本地配置文件%s读取失败:%s", m.confUrl, err.Error()))
				return
			}
		}
		m.conf = koanf.New(".")
		err = m.conf.Load(rawbytes.Provider(confData), yaml.Parser())
		if err != nil {
			logs.Error("MySQL配置格式解析错误:" + err.Error())
			m.conf = nil
			return
		}
		m.mysql, _ = gorm.Open(mysql.Open(m.conf.String("go.data.mysql")), &gorm.Config{})
		if m.conf.Bool("go.data.mysql_debug") {
			m.mysql = m.mysql.Debug()
		}
		if m.conf.Int("go.data.mysql_pool.max") > 1 {
			max := m.conf.Int("go.data.mysql_pool.max")
			if max < 10 {
				max = 10
			}
			idle := m.conf.Int("go.data.mysql_pool.total")
			if idle == 0 || idle < max {
				idle = 5 * max
			}
			idleTimeout := m.conf.Int("go.data.mysql_pool.timeout")
			if idleTimeout == 0 {
				idleTimeout = 60
			}
			lifetime := m.conf.Int("go.data.mysql_pool.life")
			if lifetime == 0 {
				lifetime = 60
			}
			sqldb, _ := m.mysql.DB()
			sqldb.SetConnMaxIdleTime(time.Duration(idleTimeout) * time.Second)
			sqldb.SetMaxIdleConns(idle)
			sqldb.SetMaxOpenConns(max)
			sqldb.SetConnMaxLifetime(time.Duration(lifetime) * time.Minute)
		}
	}
}

func (m *MysqlClient) Close() {
	sqldb, _ := m.mysql.DB()
	sqldb.Close()
	m.mysql = nil
}

func mySqlCheck(m *MysqlClient) (*gorm.DB, error) {
	if m.mysql == nil {
		m.Init("")
		if m.mysql == nil {
			return nil, errors.New("mySQL connection error")
		}
	}
	sqldb, _ := m.mysql.DB()
	err := sqldb.Ping()
	if err != nil {
		m.Close()
		m.Init("")
		if m.mysql == nil {
			return nil, errors.New("mySQL connection error")
		}
	}
	return m.mysql, nil
}

func (m *MysqlClient) Check() error {
	var err error
	_, err = mySqlCheck(m)
	if err != nil {
		logs.Error(err.Error())
	}
	return err
}

func (m *MysqlClient) GetConnection() (*gorm.DB, error) {
	return mySqlCheck(m)
}
