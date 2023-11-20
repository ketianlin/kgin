package config

import (
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/sadlil/gologger"
	"os"
	"path/filepath"
	"strings"
)

type config struct {
	Cnf    *koanf.Koanf
	App    app       `json:"app"`
	Config appConfig `json:"config"`
	Logger appLogger `json:"logger"`
}

type app struct {
	Name   string `json:"name"`
	Port   int    `json:"port"`
	Debug  bool   `json:"debug"`
	IpAddr string `json:"ipAddr"`
}

type appLogger struct {
	Level string `json:"level"`
	Out   string `json:"out"`
	File  string `json:"file"`
}

type appConfig struct {
	Server string `json:"server"`
	Type   string `json:"type"`
	Path   string `json:"path"`
	Mid    string `json:"mid"`
	Env    string `json:"env"`
	Used   string `json:"used"`
	Prefix struct {
		Mysql string `json:"mysql"`
		Redis string `json:"redis"`
	} `json:"prefix"`
}

var Config = &config{}

var logger = gologger.GetLogger()

const YmlFile = "./application.yml"

func (c *config) GetConfigUrl(prefix string) string {
	configUrl := c.Config.Server
	switch c.Config.Type {
	case "file":
		path, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		if c.Config.Path != "" {
			path = strings.TrimSuffix(c.Config.Path, "/")
		}
		configUrl = path + "/" + prefix + "-" + c.Config.Env + ".yml"
	default:
		configUrl = configUrl + prefix + "-" + c.Config.Env + ".yml"
	}
	return configUrl
}

func (c *config) GetConfigString(name string) string {
	if c.Cnf == nil {
		return ""
	}
	if c.Cnf.Exists(name) {
		return c.Cnf.String(name)
	}
	return ""
}

func (c *config) GetConfigInt(name string) int {
	if c.Cnf == nil {
		return 0
	}
	if c.Cnf.Exists(name) {
		return c.Cnf.Int(name)
	}
	return 0
}

func (c *config) GetConfigBool(name string) bool {
	if c.Cnf == nil {
		return false
	}
	if c.Cnf.Exists(name) {
		return c.Cnf.Bool(name)
	}
	return false
}

func (c *config) Init(cf string) {
	if cf == "" {
		cf = YmlFile
	}
	logger.Debug("读取配置文件:" + cf)
	c.Cnf = koanf.New(".")
	f := file.Provider(cf)
	err := c.Cnf.Load(f, yaml.Parser())
	if err != nil {
		logger.Error("读取配置文件错误:" + err.Error())
	}
	c.App.Name = c.Cnf.String("go.application.name")
	c.App.Port = c.Cnf.Int("go.application.port")
	c.App.Debug = c.Cnf.Bool("go.application.debug")
	c.App.IpAddr = c.Cnf.String("go.application.ip")
	c.Config.Server = c.Cnf.String("go.config.server")
	c.Config.Type = c.Cnf.String("go.config.server_type")
	c.Config.Env = c.Cnf.String("go.config.env")
	c.Config.Mid = c.Cnf.String("go.config.mid")
	c.Config.Path = c.Cnf.String("go.config.path")
	c.Config.Used = c.Cnf.String("go.config.used")
	c.Config.Prefix.Mysql = c.Cnf.String("go.config.prefix.mysql")
	c.Config.Prefix.Redis = c.Cnf.String("go.config.prefix.redis")
	c.Logger.Level = c.Cnf.String("go.logger.level")
	c.Logger.Out = c.Cnf.String("go.logger.out")
	c.Logger.File = c.Cnf.String("go.logger.file")
}
