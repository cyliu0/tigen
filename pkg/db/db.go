package db

import (
	"database/sql"
	"fmt"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"strings"
	"sync"
)

var logger *zap.Logger

type Addr struct {
	Host string
	Port int
	User string
	Pass string
}

func (a Addr) Dsn(db string, params ...string) string {
	dsn := fmt.Sprintf("%s:%s@%s:%d/%s", a.User, a.Pass, a.Host, a.Port, db)
	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}
	return dsn
}

type MysqlClient struct {
	Addr Addr
	Schema string
}

func (m MysqlClient) Open() (*sql.DB, error) {
	db, err := sql.Open("mysql", m.Addr.Dsn(""))
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(fmt.Sprintf("create database `%s`", m.Schema))
	if err != nil {
		return nil, err
	}
	err = db.Close()
	if err != nil {
		logger.Error("db.Close failed", zap.Error(err))
	}
	return sql.Open("mysql", m.Addr.Dsn(m.Schema))
}

func (m MysqlClient) GenTableWithData(name string, columnCount int, rowCount int, threadCount int) {
	wg := sync.WaitGroup{}
	wg.Add(threadCount)
	for i:=0; i < threadCount; i++ {
		go func(i int) {
			db, err := m.Open()
			if err != nil {
				logger.Error("m.Open failed", zap.Error(err))
				os.Exit(2)
			}
			defer db.Close()
			createStmt, types := genCreateStmt(name, columnCount, true)
			err = createTable(db, createStmt)
			if err != nil {
				logger.Error("createTable failed", zap.Error(err))
				os.Exit(3)
			}
			batch := 1000
			insertTimes := rowCount / batch
			if rowCount % batch != 0 {
				insertTimes += 1
			}
			for i := 0; i < insertTimes; i++ {
				insertRowCount := batch
				if i+1 == insertTimes && rowCount%batch != 0 {
					insertRowCount = rowCount % batch
				}
				insertStmt := genInsertStmt(name, insertRowCount, types)
				_, err := db.Exec(insertStmt)
				if err != nil {
					logger.Error("db.Exec", zap.Error(err))
					os.Exit(4)
				}
			}
		}(i)
	}
}

func genInsertStmt(name string, insertRowCount int, types []string) string {
	rowStmts := make([]string, 0)
	for i:=0; i < insertRowCount; i++ {
		rowStmt := ""
		for _, t := range types {
			if t == "varchar(100)" {
				rowStmt += fmt.Sprintf("\"%s\"", randStringBytesMaskImprSrcSB(20))
			} else if t == "int" {
				rowStmt += fmt.Sprintf("%d", rand.Int())
			}
		}
		rowStmt = "(" + rowStmt + ")"
		rowStmts = append(rowStmts, rowStmt)
	}
	return fmt.Sprintf("insert into `%s` values%s", name, strings.Join(rowStmts, ","))
}

func randType() string {
	types := []string{"int", "varchar(100)"}
	return types[rand.Int()%2]
}

func randStringBytesMaskImprSrcSB(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	sb := strings.Builder{}
	sb.Grow(n)
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

func genCreateStmt(name string, columnCount int, primaryKey bool) (string, []string){
	columnStmts := make([]string, 0)
	types := make([]string, 0)
	if primaryKey {
		columnStmts = append(columnStmts, "pk int auto_increment primary key")
		types = append(types, "int")
	}
	for i:=len(columnStmts); i < columnCount; i++ {
		colType := randType()
		types = append(types, colType)
		colStmt := fmt.Sprintf("col_%d %s", i, colType)
		columnStmts = append(columnStmts, colStmt)
	}
	createStmt := fmt.Sprintf("create table `%s` (%s)", name, strings.Join(columnStmts, ","))
	return createStmt, types
}

func createTable(db *sql.DB, createStmt string) error {
	_, err := db.Exec(createStmt)
	return err
}
