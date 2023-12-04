package db

import (
	"github.com/ketianlin/kgin/db/mongo"
	"github.com/ketianlin/kgin/db/mysql"
	"github.com/ketianlin/kgin/db/redis"
	"github.com/ketianlin/kgin/db/sqlite"
)

var Redis = new(redis.RedisClient)
var Mysql = new(mysql.MysqlClient)
var Mongo = new(mongo.MongoDB)
var Sqlite = new(sqlite.Sqlite)
