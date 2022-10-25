package mysql

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"sync"
	"time"
)

var Pool *client.Pool
var oncePool sync.Once
var TestDBPool *client.Pool
var onceTestDBPool sync.Once

type DBConfig struct {
	Addr     string
	User     string
	Password string
	DBName   string
}

func NewConnPool(cfg DBConfig) {
	oncePool.Do(func() {
		Pool = client.NewPool(func(format string, args ...interface{}) {
			println(fmt.Sprint(args)) // 增加日志方法
		}, 1, 5, 5, cfg.Addr, cfg.User, cfg.Password, cfg.DBName)
	})
}

func NewConnTestPool(cfg DBConfig) {
	onceTestDBPool.Do(func() {
		TestDBPool = client.NewPool(func(format string, args ...interface{}) {
			println(fmt.Sprint(args)) // 增加日志方法
		}, 1, 5, 5, cfg.Addr, cfg.User, cfg.Password, "test")
	})
}

func GetShowCreateTableSql(tName string) (string, error) {
	sql := ""
	err := QueryForRow(fmt.Sprintf("show CREATE TABLE `%s`", tName), nil, &sql)
	if err != nil {
		return "", err
	}
	return sql, err
}

func Execute(stmt string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	conn, err := Pool.GetConn(ctx)
	if err != nil {
		return err
	}
	defer Pool.PutConn(conn)
	_, err = conn.Execute(stmt)
	return err
}

func ExecuteTestDB(stmt string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	conn, err := TestDBPool.GetConn(ctx)
	if err != nil {
		return err
	}
	defer TestDBPool.PutConn(conn)
	_, err = conn.Execute(stmt)
	return err
}

func QueryForSlice(stmt string, rslIndex int) (rsl []string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	conn, err := Pool.GetConn(ctx)
	if err != nil {
		return nil, err
	}
	defer Pool.PutConn(conn)
	r, err := conn.Execute(stmt)
	if err != nil {
		return nil, err
	}
	for i := 0; i < r.RowNumber(); i++ {
		data, err := r.GetString(i, rslIndex)
		if err != nil {
			return nil, err
		}
		rsl = append(rsl, data)
	}
	return
}

func QueryForRow(stmt string, rslData ...interface{}) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	conn, err := Pool.GetConn(ctx)
	if err != nil {
		return err
	}
	defer Pool.PutConn(conn)

	err = conn.Ping()
	if err != nil {
		return err
	}
	r, err := conn.Execute(stmt)
	if err != nil {
		return err
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
				return fmt.Errorf("只支持，nil,*uint64,*int64,*string类型")
			}
		}
	}
	return
}
