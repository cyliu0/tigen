package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"strings"
	"sync"
)

type Addr struct {
	Host string
	Port int
	User string
	Pass string
}

func (a Addr) Dsn(db string, params ...string) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", a.User, a.Pass, a.Host, a.Port, db)
	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}
	return dsn
}

type MysqlClient struct {
	Addr   Addr
	Schema string
	Logger *zap.Logger
}

func (m MysqlClient) Open() (*sql.DB, error) {
	db, err := sql.Open("mysql", m.Addr.Dsn(""))
	if err != nil {
		m.Logger.Error("sql.Open failed", zap.String("dsn", m.Addr.Dsn("")), zap.Error(err))
		return nil, err
	}
	stmt := fmt.Sprintf("create database if not exists `%s`", m.Schema)
	_, err = db.Exec(stmt)
	if err != nil {
		m.Logger.Error("db.Exec failed", zap.String("stmt", stmt), zap.Error(err))
		return nil, err
	}
	err = db.Close()
	if err != nil {
		m.Logger.Error("db.Close failed", zap.Error(err))
	}
	return sql.Open("mysql", m.Addr.Dsn(m.Schema))
}

func (m MysqlClient) GenTableWithData(name string, columnCount, rowCount, threadCount, batch int,) {
	createStmt, types := genCreateStmt(name, columnCount, true)
	db, err := m.Open()
	if err != nil {
		m.Logger.Error("m.Open failed", zap.Error(err))
		os.Exit(2)
	}
	_, err = db.Exec(fmt.Sprintf("drop table if exists `%s`", name))
	err = createTable(db, createStmt)
	if err != nil {
		m.Logger.Error("createTable failed", zap.Error(err))
		os.Exit(3)
	}
	insertCount := rowCount / threadCount
	leftCount := rowCount % threadCount
	wg := sync.WaitGroup{}
	wg.Add(threadCount)
	for i:=0; i < threadCount; i++ {
		go func(i int) {
			db, err := m.Open()
			if err != nil {
				m.Logger.Error("m.Open failed", zap.Error(err))
				os.Exit(2)
			}
			defer db.Close()
			m.batchInsert(insertCount, batch, db, name, types)
			_ = db.Close()
			wg.Done()
		}(i)
	}
	wg.Wait()
	m.batchInsert(leftCount, batch, db, name, types)
}

func (m MysqlClient) batchInsert(insertCount, batch int, db *sql.DB, name string, types map[string]string) {
	insertTimes := insertCount / batch
	if insertCount % batch != 0 {
		insertTimes += 1
	}
	for i := 0; i < insertTimes; i++ {
		insertRowCount := batch
		if i+1 == insertTimes && insertCount % batch != 0 {
			insertRowCount = insertCount % batch
		}
		insertStmt := genInsertStmt(name, insertRowCount, types)
		_, err := db.Exec(insertStmt)
		if err != nil {
			m.Logger.Error("db.Exec", zap.Error(err), zap.String("insertStmt", insertStmt))
			os.Exit(4)
		}
	}
}

func genInsertStmt(name string, insertRowCount int, types map[string]string) string {
	cols := make([]string, 0)
	colsStmt := "("
	for i :=2; i < len(types) + 2; i++ {
		col := fmt.Sprintf("col_%d", i)
		cols = append(cols, col)
		if colsStmt != "(" {
			colsStmt += ","
		}
		colsStmt += "`" + col + "`"
	}
	colsStmt += ")"
	rowStmts := make([]string, 0)
	for i:=0; i < insertRowCount; i++ {
		rowStmt := ""
		for _, col := range cols {
			t := types[col]
			if rowStmt != "" {
				rowStmt += ","
			}
			if t == "varchar(100)" {
				rowStmt += fmt.Sprintf("\"%s\"", randStringBytesMaskImprSrcSB(20))
			} else if t == "int" {
				rowStmt += fmt.Sprintf("%d", rand.Int31())
			}
		}
		rowStmt = "(" + rowStmt + ")"
		rowStmts = append(rowStmts, rowStmt)
	}

	return fmt.Sprintf("insert into `%s` %s values%s", name, colsStmt, strings.Join(rowStmts, ","))
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

func genCreateStmt(name string, columnCount int, primaryKey bool) (string, map[string]string){
	columnStmts := make([]string, 0)
	types := make(map[string]string, 0)
	if primaryKey {
		columnStmts = append(columnStmts, "pk int auto_increment primary key")
	}
	for i:=len(columnStmts); i < columnCount; i++ {
		colType := randType()
		colId := fmt.Sprintf("col_%d", i+1)
		types[colId] = colType
		colStmt := fmt.Sprintf("%s %s", colId, colType)
		columnStmts = append(columnStmts, colStmt)
	}
	createStmt := fmt.Sprintf("create table `%s` (%s)", name, strings.Join(columnStmts, ","))
	return createStmt, types
}

func createTable(db *sql.DB, createStmt string) error {
	_, err := db.Exec(createStmt)
	return err
}
