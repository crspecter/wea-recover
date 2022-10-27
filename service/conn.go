package service

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"strconv"
	"strings"
	"time"
	"wea-recover/common"
	"wea-recover/common/def"
	mysql2 "wea-recover/mysql"
)

type db struct {
	addr       string
	port       int
	user       string
	pwd        string
	db         string
	table      string
	schema     *schema.Table
	clientPool *client.Pool
	TestDBPool *client.Pool
}

func (d *db) Init(param def.InputInfo) error {
	d.addr = param.Addr
	d.port = param.Port
	d.user = param.User
	d.pwd = param.Pwd
	d.db = param.Db
	d.table = param.Table

	//构建链接需要恢复数据库连接池
	mysql2.NewConnPool(mysql2.DBConfig{
		Addr:     d.addr + ":" + strconv.Itoa(d.port),
		User:     d.user,
		Password: d.pwd,
		DBName:   d.db,
	})

	d.clientPool = mysql2.Pool

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	conn, err := d.clientPool.GetConn(ctx)
	if err != nil {
		return common.Error("GetConn err:", err)
	}
	defer d.clientPool.PutConn(conn)
	_, err = conn.Execute("CREATE DATABASE IF NOT EXISTS test;")
	if err != nil {
		return common.Error("CREATE DATABASE test err:", err)
	}

	// 创建执行sql的连接池test库连接池
	mysql2.NewConnTestPool(mysql2.DBConfig{
		Addr:     d.addr + ":" + strconv.Itoa(d.port),
		User:     d.user,
		Password: d.pwd,
	})
	d.TestDBPool = mysql2.TestDBPool

	err = d.updateSchema()
	if err != nil {
		return err
	}

	sql, err := d.getShowCreateTableSql(d.table)
	if err != nil {
		return common.Error("getShowCreateTableSql err:", err)
	}

	//创建test表
	sql = strings.Replace(sql, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`", d.table), fmt.Sprintf("CREATE TABLE IF NOT EXISTS `test`.`%s`", d.table+"_recover"), 1)
	var rsl *mysql.Result
	rsl, err = conn.Execute(sql)
	if err != nil {
		return common.Error("执行建表语句执行失败 err:", err)
	}
	rsl.Close()

	sql = fmt.Sprintf("TRUNCATE TABLE `test`.`%s`", d.table+"_recover")
	_, err = conn.Execute(sql)
	if err != nil {
		return common.Error("清空表语句执行失败 err:", err)
	}

	return nil
}

func (d *db) UseDBName(dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	conn, err := d.clientPool.GetConn(ctx)
	if err != nil {
		return common.Error("GetConn err:", err)
	}
	defer d.clientPool.PutConn(conn)
	_, err = conn.Execute(fmt.Sprintf(`use %s`, dbName))
	if err != nil {
		return err
	}
	d.db = dbName
	return nil
}

func (d *db) updateSchema() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	conn, err := d.clientPool.GetConn(ctx)
	if err != nil {
		return common.Error("GetConn err:", err)
	}
	defer d.clientPool.PutConn(conn)
	t, err := schema.NewTable(conn, d.db, d.table)
	if err != nil {
		return common.Error("NewTable err:", err)
	}
	d.schema = t
	return nil
}

func (d *db) getShowCreateTableSql(tName string) (string, error) {
	sql := ""
	err := d.QueryForRow(fmt.Sprintf("show CREATE TABLE `%s`", tName), nil, &sql)
	if err != nil {
		return "", err
	}
	sql = strings.Replace(sql, fmt.Sprintf("CREATE TABLE `%s`", tName), fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`", tName), 1)
	return sql, err
}

func (d *db) QueryForRow(stmt string, rslData ...interface{}) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	conn, err := d.clientPool.GetConn(ctx)
	if err != nil {
		return common.Error("GetConn err:", err)
	}
	defer d.clientPool.PutConn(conn)

	err = conn.Ping()
	if err != nil {
		return common.Error("Ping err:", err)
	}
	r, err := conn.Execute(stmt)
	if err != nil {
		return common.Error("Execute", stmt, "err:", err)
	}
	for i := 0; i < r.RowNumber(); i++ {
		for j := 0; j < r.ColumnNumber() && j < len(rslData); j++ {
			if rslData[j] == nil {
				continue
			}
			switch v := rslData[j].(type) {
			case *uint64:
				*v, err = r.GetUint(i, j)
				if err != nil {
					return err
				}
			case *int64:
				*v, err = r.GetInt(i, j)
				if err != nil {
					return err
				}
			case *string:
				*v, err = r.GetString(i, j)
				if err != nil {
					return err
				}
			default:
				return common.Error("只支持，nil,*uint64,*int64,*string类型")
			}
		}
	}
	return
}

func (d *db) GetSchema() *schema.Table {
	return d.schema
}
