package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"log"
	"wea-recover/common/def"
	"wea-recover/service"
)

func parseParam() def.InputInfo {
	addr := pflag.StringP("addr", "h", "127.0.0.1", "MySQL IP")
	port := pflag.IntP("port", "P", 0, "MySQL port")
	pwd := pflag.StringP("pwd", "p", "", "MySQL password")
	db := pflag.StringP("db", "D", "", "MySQL database")
	table := pflag.StringP("table", "t", "", "MySQL table")
	binlog := pflag.StringP("binlog", "", "", "dump模式下起始binlog")
	binlog_path := pflag.StringP("binlog-path", "", "", "文件模式下,binlog集合")
	start_datetime := pflag.StringP("start-datetime", "", "", "恢复起始时间")
	stop_datetime := pflag.StringP("stop-datetime", "", "", "恢复截止时间")

	start_position := pflag.Uint32P("start-position", "", 0, "恢复起始位点")
	stop_position := pflag.Uint32P("stop-position", "", 0, "恢复截止位点")
	export := pflag.Bool("export", false, "是否导出表到当前目录下export.sql文件中")
	pflag.Parse()

	ret := def.InputInfo{
		Addr:          *addr,
		Port:          *port,
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
	return ret
}

func initEnv() error {
	return nil
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
