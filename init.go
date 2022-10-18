package main

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/spf13/pflag"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"wea-recover/common/def"
	"wea-recover/service"
)

func parseParam() (def.InputInfo, error) {
	addr := pflag.StringP("addr", "h", "127.0.0.1", "MySQL IP")
	port := pflag.IntP("port", "P", 0, "MySQL port")
	user := pflag.StringP("user", "u", "", "MySQL user")
	pwd := pflag.StringP("pwd", "p", "", "MySQL password")
	db := pflag.StringP("db", "D", "", "MySQL database")
	table := pflag.StringP("table", "t", "", "MySQL table")
	binlog := pflag.StringP("binlog", "", "", "dump模式下起始binlog")
	binlog_path := pflag.StringP("binlog-path", "", "", "文件模式下,binlog集合,eg: /path/file1,/path/file2")
	start_datetime := pflag.StringP("start-datetime", "", "", "恢复起始时间,eg:2006-01-02_15:04:05")
	stop_datetime := pflag.StringP("stop-datetime", "", "", "恢复截止时间,eg:2006-01-02_15:04:05")
	start_position := pflag.Uint32P("start-position", "", 0, "恢复起始位点")
	stop_position := pflag.Uint32P("stop-position", "", 0, "恢复截止位点")
	export := pflag.Bool("export", false, "是否导出表到当前目录下export.sql文件中")
	pflag.Parse()

	if net.ParseIP(*addr) == nil {
		return def.InputInfo{}, fmt.Errorf("MySQL地址格式不正确")
	}
	if *port < 1000 || *port > 65535 {
		return def.InputInfo{}, fmt.Errorf("端口格式不正确")
	}
	if strings.TrimSpace(*db) == "" {
		return def.InputInfo{}, fmt.Errorf("数据库名不能为空")
	}
	if strings.TrimSpace(*table) == "" {
		return def.InputInfo{}, fmt.Errorf("数据库表不能为空")
	}
	if strings.TrimSpace(*binlog) == "" && strings.TrimSpace(*binlog_path) == "" {
		return def.InputInfo{}, fmt.Errorf("binlog和binlog_path选其一填入,才能进行数据恢复")
	}
	if strings.TrimSpace(*binlog) != "" && strings.TrimSpace(*binlog_path) != "" {
		return def.InputInfo{}, fmt.Errorf("binlog和binlog_path选其一填入,才能进行数据恢复")
	}
	if strings.TrimSpace(*start_datetime) != "" {
		if _, err := time.Parse("2006-01-02_15:04:05", *start_datetime); err != nil {
			return def.InputInfo{}, fmt.Errorf("时间格式不正确,eg:2006-01-02_15:04:05")
		}
	}
	if strings.TrimSpace(*stop_datetime) != "" {
		if _, err := time.Parse("2006-01-02_15:04:05", *stop_datetime); err != nil {
			return def.InputInfo{}, fmt.Errorf("时间格式不正确,eg:2006-01-02_15:04:05")
		}
	}
	if strings.TrimSpace(*binlog_path) != "" {
		arr := strings.Split(*binlog_path, ",")
		for _, v := range arr {
			if !isFile(v) {
				return def.InputInfo{}, fmt.Errorf("binlog文件不存在:%s", v)
			}
		}
	}

	conn, err := client.Connect(*addr+":"+strconv.Itoa(*port), *user, *pwd, *db)
	if err != nil {
		return def.InputInfo{}, fmt.Errorf("mysql连接失败:%s", err.Error())
	}
	if err := conn.Ping(); err != nil {
		return def.InputInfo{}, fmt.Errorf("mysql ping失败:%s", err.Error())
	}

	ret := def.InputInfo{
		Addr:          *addr,
		Port:          *port,
		User:          *user,
		Pwd:           *pwd,
		Db:            *db,
		Table:         *table,
		Binlog:        *binlog,
		BinlogPath:    *binlog_path,
		StartDatetime: *start_datetime,
		StopDatetime:  *stop_datetime,
		StartPosition: *start_position,
		StopPosition:  *stop_position,
		Export:        *export,
	}
	fmt.Println(ret)
	return ret, nil
}

func run(param def.InputInfo) {
	var err error
	if param.Binlog != "" || param.BinlogPath != "" {
		r := service.NewRecover(param)
		if r == nil {
			err = fmt.Errorf("new recover fail")
		} else {
			err = r.Run()
		}
	}

	//导出sql
	if err == nil && param.Export {
		err = service.Export(param)
	}

	if err != nil {
		fmt.Println("恢复失败:", err)
		log.Fatal("恢复失败:", err)
	}
}

func isFile(path string) bool {
	file, err := os.Stat(path)
	if err != nil {
		log.Println("E find file error ", err.Error())
		return false
	}
	if file.IsDir() {
		log.Println("E find dir not file ", path)
		return false
	}

	return true
}
