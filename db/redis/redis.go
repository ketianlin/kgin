package redis

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/ketianlin/kgin/logs"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"io/ioutil"
	"net"
	"strings"
	"time"
)

type RedisClient struct {
	client  *redis.Client
	conf    *koanf.Koanf
	confUrl string
}

func (r *RedisClient) Init(redisConfigUrl string) {
	if redisConfigUrl != "" {
		r.confUrl = redisConfigUrl
	}
	if r.confUrl == "" {
		logs.Error("Redis配置Url为空")
		return
	}
	if r.client == nil {
		var confData []byte
		var err error
		if strings.HasPrefix(r.confUrl, "http://") {
			resp, err := grequests.Get(r.confUrl, nil)
			if err != nil {
				logs.Error("Redis配置下载失败! " + err.Error())
				return
			}
			confData = []byte(resp.String())
		} else {
			confData, err = ioutil.ReadFile(r.confUrl)
			if err != nil {
				logs.Error(fmt.Sprintf("Redis本地配置文件%s读取失败:%s", r.confUrl, err.Error()))
				return
			}
		}
		r.conf = koanf.New(".")
		err = r.conf.Load(rawbytes.Provider(confData), yaml.Parser())
		if err != nil {
			logs.Error("Redis配置文件解析错误:" + err.Error())
			r.conf = nil
			return
		}

		var ro redis.Options
		ro = redis.Options{
			Addr:     r.conf.String("go.data.redis.host") + ":" + r.conf.String("go.data.redis.port"),
			Password: r.conf.String("go.data.redis.password"),
			DB:       r.conf.Int("go.data.redis.database"),
			Dialer: func() (net.Conn, error) {
				netDialer := &net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 5 * time.Minute,
				}
				return netDialer.Dial("tcp", r.conf.String("go.data.redis.host")+":"+r.conf.String("go.data.redis.port"))
			},
		}
		if r.conf.Int("go.data.redis_pool.max") > 1 {
			min := r.conf.Int("go.data.redis_pool.min")
			if min == 0 {
				min = 2
			}
			max := r.conf.Int("go.data.redis_pool.max")
			if max < 10 {
				max = 10
			}
			idleTimeout := r.conf.Int("go.data.redis_pool.idleTimeout")
			if idleTimeout == 0 {
				idleTimeout = 5
			}
			connectTimeout := r.conf.Int("go.data.redis_pool.timeout")
			if connectTimeout == 0 {
				connectTimeout = 60
			}
			ro.PoolSize = max
			ro.MinIdleConns = min
			ro.IdleTimeout = time.Duration(idleTimeout) * time.Minute
			ro.DialTimeout = time.Duration(connectTimeout) * time.Second
		}
		r.client = redis.NewClient(&ro)
		if err := r.client.Ping().Err(); err != nil {
			logs.Error("Redis连接失败:" + err.Error())
		}
	}
}

func (r *RedisClient) Close() {
	r.client.Close()
	r.client = nil
}

func (r *RedisClient) GetConnection() *redis.Client {
	return r.client
}

func (r *RedisClient) Check() error {
	return nil
}
