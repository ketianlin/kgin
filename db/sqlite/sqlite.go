package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ketianlin/kgin/logs"
	_ "github.com/mattn/go-sqlite3"
)

type Sqlite struct {
	sqlite *sql.DB
	dbFile string
}

func (m *Sqlite) Init(dbFileName string) {
	//path, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	//if dbFileName == "" {
	//	dbFileName = fmt.Sprintf("%s/%s.db", path, config.Config.App.Name)
	//} else if !(dbFileName[:1] == "/" || dbFileName[1:2] == ":") {
	//	dbFileName = fmt.Sprintf("%s/%s", path, dbFileName)
	//}
	//m.dbFile = dbFileName
	m.dbFile = fmt.Sprintf("%s.db", dbFileName)
	if m.sqlite == nil {
		var err error
		m.sqlite, err = sql.Open("sqlite3", m.dbFile)
		if err != nil {
			logs.Error("打开sqlite数据库出错：{}", err.Error())
		}
	}
}

func (m *Sqlite) Close() {
	if m.sqlite != nil {
		err := m.sqlite.Close()
		if err != nil {
			logs.Error("关闭sqlite数据库出错：{}", err.Error())
			return
		}
		m.sqlite = nil
	}
}

func (m *Sqlite) Check() error {
	return nil
}

func (m *Sqlite) GetConnection() (*sql.DB, error) {
	if m.sqlite == nil {
		return nil, errors.New("SQLite not opened")
	}
	return m.sqlite, nil
}
