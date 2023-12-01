package test

import (
	"context"
	"fmt"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/db"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"math/rand"
	"testing"
	"time"
)

type UserBasic struct {
	Account   string             `bson:"account"`
	Password  string             `bson:"password"`
	Nickname  string             `bson:"nickname"`
	Sex       int                `bson:"sex"`
	Email     string             `bson:"email"`
	Avatar    string             `bson:"avatar"`
	Age       int                `bson:"age"`
	Score     float64            `bson:"score"`
	CreatedAt int64              `bson:"created_at"`
	UpdatedAt int64              `bson:"updated_at"`
	Id        primitive.ObjectID `bson:"_id,omitempty"`
	//field.DefaultField `bson:",inline"`   // 这个可以自动化更新上面3个fields
}

// 指定自定义field的field名
//func (u *UserBasic) CustomFields() field.CustomFieldsBuilder {
//	return field.NewCustom().SetCreateAt("CreateTimeAt").SetUpdateAt("UpdateTimeAt").SetId("MyId")
//}

func TestMongo(t *testing.T) {
	//初始化配置
	configFile := "/home/ke666/my_codes/go_codes/kgin/test/kgin.yml"
	fmt.Println(configFile)
	kgin.Init(configFile)
	fmt.Println("11111111111")
	// 新增测试
	//addOneUserMongoDB() // 新增1个
	//addManyUserMongoDB() // 批量新增
	// 更新测试
	//updateByIdUserMongoDB() // 更新一条
	//updateOneUserMongoDB() // 更新一条
	//updateManyUserMongoDB() // 更新多条
	//batchUpdateUserScoreMongoDB() // 批量更新分数
	// 查询测试
	//findOneUserMongoDB() // 查找一个文档
	//findPageUserMongoDB() // 查找分页文档
	//aggregateUserMongoDB() // 聚合查找文档
	// 删除文档
	//deleteUserMongoDB() // 删除文档
	// 事务测试 事务的连接初始化和其他的不一样
	transactionUserMongoDB() // 事务操作
	select {}
}

func (UserBasic) CollectionName() string {
	return "user_basic"
}

// 事务 操作 开始 --------------------------------
func transactionUserMongoDB() {
	client, err := db.Mongo.GetTransactionClient("user_basic")
	if err != nil {
		fmt.Println("transaction error: ", err.Error())
		return
	}
	fmt.Println(client)
}

// 事务 操作 结束 --------------------------------

// delete 操作 开始 --------------------------------
func deleteUserMongoDB() {
	userColl := db.Mongo.GetConnection().Collection("user_basic")
	id, _ := primitive.ObjectIDFromHex("65698ceabad3cebf77a14ad1")
	err := userColl.RemoveId(context.Background(), id)
	if err != nil {
		fmt.Println("delete error: ", err.Error())
		return
	}
	fmt.Println("删除成功")
}

// delete 操作 结束 --------------------------------

// search 操作 开始 --------------------------------
func aggregateUserMongoDB() {
	userColl := db.Mongo.GetConnection().Collection("user_basic")
	// 年龄大于96的
	matchStage := bson.D{{"$match", []bson.E{{"age", bson.D{{"$gt", 96}}}}}}
	// 分组后sum一下分数
	groupStage := bson.D{{"$group", bson.D{{"_id", "$age"}, {"total", bson.D{{"$sum", "$score"}}}}}}
	var showsWithInfo []bson.M
	err := userColl.Aggregate(context.Background(), qmgo.Pipeline{matchStage, groupStage}).All(&showsWithInfo)
	if err != nil {
		fmt.Println("aggregate error: ", err.Error())
		return
	}
	for _, m := range showsWithInfo {
		fmt.Printf("%T\t%#v\n", m, m)
	}
}

func findPageUserMongoDB() {
	page := 3
	pageSize := 5
	userColl := db.Mongo.GetConnection().Collection("user_basic")
	// 查询有几条
	count, err := userColl.Find(context.Background(), bson.D{{"sex", 1}}).Count()
	if err != nil {
		fmt.Println("Count error: ", err.Error())
		return
	}
	users := []UserBasic{}
	skip := int64((page - 1) * pageSize)
	err = userColl.Find(context.Background(), bson.D{{"sex", 1}}).Sort("account").Skip(skip).Limit(int64(pageSize)).All(&users)
	if err != nil {
		fmt.Println("All error: ", err.Error())
		return
	}
	fmt.Println("count: ", count)
	for _, user := range users {
		fmt.Println(user)
	}
}

func findOneUserMongoDB() {
	userColl := db.Mongo.GetConnection().Collection("user_basic")
	user := new(UserBasic)
	err := userColl.Find(context.Background(), bson.M{"account": "diaomao-7"}).One(user)
	if err != nil {
		fmt.Println("find error: ", err.Error())
		return
	}
	fmt.Println(user)
}

// search 操作 结束 --------------------------------

// update 操作 开始 --------------------------------
func batchUpdateUserScoreMongoDB() {
	userColl := db.Mongo.GetConnection().Collection("user_basic")
	users := []UserBasic{}
	err := userColl.Find(context.TODO(), bson.D{}).All(&users)
	if err != nil {
		fmt.Println("batch update all error: ", err.Error())
		return
	}
	for _, user := range users {
		rand.Seed(time.Now().UnixNano())
		score := rand.Intn(101) // 生成1-100之间的分数
		update := bson.D{
			{"$set", bson.D{{"score", score}, {"updated_at", time.Now().UnixMilli()}}}, // 更新account,time
		}
		err := userColl.UpdateId(context.Background(), user.Id, update)
		if err != nil {
			fmt.Println("batch update error: ", err.Error(), ", id: ", user.Id)
		}
	}
	fmt.Println("batch done")
}

func updateManyUserMongoDB() {
	userCollection := db.Mongo.GetConnection().Collection("user_basic")
	filter := bson.D{
		{"$and",
			bson.A{
				bson.D{{"age", bson.D{{"$gt", 98}}}},
			},
		},
	}
	update := bson.D{
		{"$set", bson.D{{"age", 97}, {"updated_at", time.Now().UnixMilli()}}},
	}
	all, err := userCollection.UpdateAll(context.TODO(), filter, update)
	if err != nil {
		fmt.Println("update all error: ", err.Error())
		return
	}
	fmt.Println("updateMany 更新ok:", all)
}

func updateOneUserMongoDB() {
	userCollection := db.Mongo.GetConnection().Collection("user_basic")
	filter := bson.D{
		{"$and",
			bson.A{
				bson.D{{"age", bson.D{{"$gt", 99}}}},
			},
		},
	}
	update := bson.D{
		{"$set", bson.D{{"age", 97}, {"updated_at", time.Now().UnixMilli()}}},
	}
	err := userCollection.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		fmt.Println("update error: ", err.Error())
		return
	}
	fmt.Println("updateOne 更新ok")
}

func updateByIdUserMongoDB() {
	update := bson.D{
		{"$set", bson.D{{"account", "diaomao-4-up"}, {"updated_at", time.Now().UnixMilli()}}}, // 更新account,time
		{"$inc", bson.D{{"age", 99}}}, // 增加age字段
	}
	userCollection := db.Mongo.GetConnection().Collection("user_basic")
	_id, _ := primitive.ObjectIDFromHex("65698d078ac2fcaebd544d5b")
	err := userCollection.UpdateId(context.TODO(), _id, update)
	if err != nil {
		fmt.Println("update error: ", err.Error())
		return
	}
	fmt.Println("update 更新ok")
}

// update 操作 结束 --------------------------------

// add 操作 开始 --------------------------------
func addManyUserMongoDB() {
	conn := db.Mongo.GetConnection()
	userCollection := conn.Collection("user_basic")
	users := make([]interface{}, 20)
	for i := 0; i < 20; i++ {
		users[i] = bson.D{
			{"_id", primitive.NewObjectID()},
			{"account", fmt.Sprintf("diaomao-%d", i)},
			{"password", "123456"},
			{"nickname", fmt.Sprintf("吊毛-%d", i)},
			{"sex", 1},
			{"email", fmt.Sprintf("diaomao-%d@qq.com", i)},
			{"avatar", fmt.Sprintf("http://www.diaomao.com/images/dm-%d.png", i)},
			{"created_at", time.Now().UnixMilli()},
			{"updated_at", time.Now().UnixMilli()},
		}
	}
	many, err := userCollection.InsertMany(context.TODO(), users)
	if err != nil {
		fmt.Println("add many error: ", err.Error())
		return
	}
	fmt.Println("success: ", many.InsertedIDs)
}

func addOneUserMongoDB() {
	conn := db.Mongo.GetConnection()
	userCollection := conn.Collection("user_basic")
	one, err := userCollection.InsertOne(context.TODO(), UserBasic{
		Id:        primitive.NewObjectID(),
		Account:   "diaomao-666",
		Password:  "123456",
		Nickname:  "吊毛-666",
		Sex:       1,
		Email:     "diaomao666@qq.com",
		Avatar:    "http://www.diaomao.com/images/dm666.png",
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
		Score:     98,
	})
	if err != nil {
		fmt.Println("add error: ", err.Error())
		return
	}
	fmt.Println("success: ", one.InsertedID)
}

// add 操作 结束 --------------------------------
