package db

import (
	"github.com/ketianlin/kgin/db/mysql"
	"github.com/ketianlin/kgin/db/redis"
)

var Redis = new(redis.RedisClient)
var Mysql = new(mysql.MysqlClient)
