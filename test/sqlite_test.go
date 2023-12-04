package test

import (
	"fmt"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/db"
	"testing"
	"time"
)

func TestSqlite(t *testing.T) {
	/*
		建表语句
		CREATE TABLE `userinfo` (
		    `uid` INTEGER PRIMARY KEY AUTOINCREMENT,
		    `username` VARCHAR(64) NULL,
		    `department` VARCHAR(64) NULL,
		    `created` DATE NULL
		);

		CREATE TABLE `userdetail` (
		    `uid` INT(10) NULL,
		    `intro` TEXT NULL,
		    `profile` TEXT NULL,
		    PRIMARY KEY (`uid`)
		);
	*/
	//初始化配置
	configFile := "/home/ke666/my_codes/go_codes/kgin/test/kgin.yml"
	fmt.Println(configFile)
	kgin.Init(configFile)
	fmt.Println("11111111111")
	//saveUserInfo()   // 新增
	//updateUserInfo() // 更新
	//queryUserInfo() // 查询
	deleteUserInfo() // 删除
	fmt.Println("333333333333")
	select {}
}

func deleteUserInfo() {
	conn, err := db.Sqlite.GetConnection()
	checkErr(err)
	defer db.Sqlite.Close()

	stmt, err := conn.Prepare("delete from userinfo where uid=?")
	checkErr(err)

	res, err := stmt.Exec(3)
	checkErr(err)

	affect, err := res.RowsAffected()
	checkErr(err)

	fmt.Println(affect)
}

func queryUserInfo() {
	conn, err := db.Sqlite.GetConnection()
	checkErr(err)
	defer db.Sqlite.Close()

	rows, err := conn.Query("SELECT * FROM userinfo")
	checkErr(err)

	for rows.Next() {
		var uid int
		var username string
		var department string
		var created time.Time
		err = rows.Scan(&uid, &username, &department, &created)
		checkErr(err)
		fmt.Printf("uid: %v\tusername: %v\tdepartment: %v\tcreated: %v\n", uid, username, department, created)
	}
}

func updateUserInfo() {
	conn, err := db.Sqlite.GetConnection()
	checkErr(err)
	defer db.Sqlite.Close()

	stmt, err := conn.Prepare("update userinfo set username=? where uid=?")
	checkErr(err)

	res, err := stmt.Exec("diaomao22", 1)
	checkErr(err)

	affect, err := res.RowsAffected()
	checkErr(err)

	fmt.Println(affect)
}

func saveUserInfo() {
	conn, err := db.Sqlite.GetConnection()
	checkErr(err)
	defer db.Sqlite.Close()

	// 插入数据
	stmt, err := conn.Prepare("INSERT INTO userinfo(username, department, created) values(?,?,?)")
	checkErr(err)

	res, err := stmt.Exec("diaomao", "开发部", "2023-12-04")
	checkErr(err)

	id, err := res.LastInsertId()
	checkErr(err)

	fmt.Println(id)
}

func checkErr(err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("err: %v\n", r)
		}
	}()
	if err != nil {
		panic(err)
	}
}
