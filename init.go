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
	"wea-recover/common"
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
	//binlog := pflag.StringP("binlog", "", "", "dump模式下起始binlog")
	binlog_path := pflag.StringP("binlog-path", "", "", "文件模式下,binlog集合目录,eg:/path")
	start_datetime := pflag.StringP("start-datetime", "", "", "恢复起始时间,eg:2006-01-02_15:04:05")
	stop_datetime := pflag.StringP("stop-datetime", "", "", "恢复截止时间,eg:2006-01-02_15:04:05")
	start_position := pflag.StringP("start-position", "", "", "恢复起始位点信息,eg:mysql-bin.001:4")
	stop_position := pflag.StringP("stop-position", "", "", "恢复截止位点信息,eg:mysql-bin.010")
	export := pflag.Bool("export", false, "是否导出表到当前目录下export.sql文件中")
	pflag.Parse()

	*addr = strings.TrimSpace(*addr)
	*user = strings.TrimSpace(*user)
	*pwd = strings.TrimSpace(*pwd)
	*db = strings.TrimSpace(*db)
	*table = strings.TrimSpace(*table)
	*binlog_path = strings.TrimSpace(*binlog_path)
	*start_datetime = strings.TrimSpace(*start_datetime)
	*stop_datetime = strings.TrimSpace(*stop_datetime)
	*start_position = strings.TrimSpace(*start_position)
	*stop_position = strings.TrimSpace(*stop_position)

	path := *binlog_path
	if path != "" && path[len(path)-1] != '/' && path[len(path)-1] != '\\' {
		path += "/"
	}

	if net.ParseIP(*addr) == nil {
		return def.InputInfo{}, fmt.Errorf("MySQL地址格式不正确")
	}
	if *port < 1000 || *port > 65535 {
		return def.InputInfo{}, fmt.Errorf("端口格式不正确")
	}
	if *db == "" {
		return def.InputInfo{}, fmt.Errorf("数据库名不能为空")
	}
	if *table == "" {
		return def.InputInfo{}, fmt.Errorf("数据库表不能为空")
	}

	if *start_datetime != "" {
		if _, err := time.Parse("2006-01-02_15:04:05", *start_datetime); err != nil {
			return def.InputInfo{}, fmt.Errorf("时间格式不正确,eg:2006-01-02_15:04:05")
		}
	}
	if *stop_datetime != "" {
		t, err := time.Parse("2006-01-02_15:04:05", *stop_datetime)
		if err != nil {
			return def.InputInfo{}, fmt.Errorf("时间格式不正确,eg:2006-01-02_15:04:05")
		}
		cur := time.Now()
		if cur.Sub(t) < 0 {
			*stop_datetime = cur.Format("2006-01-02_15:04:05")
			common.Infoln("stop datetime err use current time:", *stop_datetime)
			fmt.Println("stop datetime err use current time:", *stop_datetime)
		}
	} else {
		*stop_datetime = time.Now().Format("2006-01-02_15:04:05")
		common.Infoln("auto add stop datetime:", *stop_datetime)
		fmt.Println("auto add stop datetime:", *stop_datetime)
	}
	if !*export && *start_position == "" {
		return def.InputInfo{}, fmt.Errorf("指定start_position,才能进行数据恢复")
	}

	var binlogs []def.BinlogPos
	ty := def.UNKNOWN
	// 解析文件模式下所有待分析的binlogs
	if path != "" {
		if *start_position == "" {
			return def.InputInfo{}, fmt.Errorf("指定start_position,才能进行数据恢复")
		}

		file, err := os.Stat(path)
		if err != nil {
			return def.InputInfo{}, fmt.Errorf("binlog目录不存在:%s", path)
		}
		if !file.IsDir() {
			return def.InputInfo{}, fmt.Errorf("%s 不是目录", path)
		}

		// 获取binlogs列表
		files, err := service.ListFile(path)
		if err != nil {
			return def.InputInfo{}, fmt.Errorf("获取文件列表失败:%s", err)
		}

		//定位binlog启止位置
		startPos := strings.Split(*start_position, ":")
		endPos := strings.Split(*stop_position, ":")

		start := -1
		end := -1
		for i, v := range files {
			if startPos[0] == v.Name() {
				start = i
			}
			if endPos[0] == v.Name() {
				end = i
				break
			}
		}
		if start == -1 {
			return def.InputInfo{}, fmt.Errorf("%s目录下没找到:%s", path, startPos[0])
		}
		if endPos[0] != "" && end == -1 {
			return def.InputInfo{}, fmt.Errorf("%s目录下没找到:%s", path, endPos[0])
		}
		if end == -1 {
			end = len(files) - 1
		}
		if start > end {
			return def.InputInfo{}, fmt.Errorf("开始binlog大于数据binlog:%s - %s", *start_position, *stop_position)
		}

		//筛选出待解析的binlog及位点信息
		for i := start; i <= end; i++ {
			var binPos def.BinlogPos

			if start == end {
				binPos.Binlog = path + startPos[0]
				if len(startPos) > 1 {
					num, err := strconv.Atoi(startPos[1])
					if err != nil {
						return def.InputInfo{}, fmt.Errorf("解析开始位点失败:%s", *start_position)
					}
					binPos.Pos = uint32(num)
				}
				binlogs = append(binlogs, binPos)

				binPos.Binlog = path + endPos[0]
				if len(endPos) > 1 {
					num, err := strconv.Atoi(endPos[1])
					if err != nil {
						return def.InputInfo{}, fmt.Errorf("解析结束位点失败:%s", *stop_position)
					}
					binPos.Pos = uint32(num)
				}
				binlogs = append(binlogs, binPos)
				break
			}

			if i == start {
				if len(startPos) > 1 {
					num, err := strconv.Atoi(startPos[1])
					if err != nil {
						return def.InputInfo{}, fmt.Errorf("解析开始位点失败:%s", *start_position)
					}
					binPos.Pos = uint32(num)
				}
			}
			if i == end {
				if len(endPos) > 1 {
					num, err := strconv.Atoi(endPos[1])
					if err != nil {
						return def.InputInfo{}, fmt.Errorf("解析结束位点失败:%s", *stop_position)
					}
					binPos.Pos = uint32(num)
				}
			}
			v := files[i]
			binPos.Binlog = path + v.Name()
			binlogs = append(binlogs, binPos)
		}
		if len(binlogs) == 0 {
			return def.InputInfo{}, fmt.Errorf("获取binlog列表失败")
		}
		ty = def.FILE_RECOVER
	} else {
		startPos := strings.Split(*start_position, ":")
		endPos := strings.Split(*stop_position, ":")

		if startPos[0] == "" {
			if *export == false {
				return def.InputInfo{}, fmt.Errorf("解析开始位点失败:%s且不是导出模式", *start_position)
			} else {
				ty = def.EXPORT_ONLY
			}
		} else {
			var binPos def.BinlogPos
			binPos.Binlog = startPos[0]
			if len(startPos) > 1 {
				num, err := strconv.Atoi(startPos[1])
				if err != nil {
					return def.InputInfo{}, fmt.Errorf("解析开始位点失败:%s", *start_position)
				}
				binPos.Pos = uint32(num)
			}
			binlogs = append(binlogs, binPos)

			if endPos[0] != "" {
				binPos.Pos = 0
				binPos.Binlog = endPos[0]
				if len(endPos) > 1 {
					num, err := strconv.Atoi(endPos[1])
					if err != nil {
						return def.InputInfo{}, fmt.Errorf("解析结束位点失败:%s", *stop_position)
					}
					binPos.Pos = uint32(num)
				}
				binlogs = append(binlogs, binPos)
			}
			if len(binlogs) == 0 {
				return def.InputInfo{}, fmt.Errorf("解析入参binlog失败")
			}
			ty = def.DUMP_RECOVER
		}
	}

	if ty == def.UNKNOWN {
		return def.InputInfo{}, fmt.Errorf("解析运行模式失败")
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
		Binlogs:       binlogs,
		StartDatetime: *start_datetime,
		StopDatetime:  *stop_datetime,
		Export:        *export,
		Ty:            ty,
	}
	common.Infoln(fmt.Sprintf("parse param: %#v", ret))
	return ret, nil
}

func run(param def.InputInfo) {
	var err error
	//数据恢复
	if param.Ty != def.EXPORT_ONLY {
		fmt.Println("数据恢复")
		r := service.NewRecover(param)
		if r == nil {
			err = fmt.Errorf("new recover fail")
		} else {
			err = r.Run()
		}
	}

	//导出sql
	if err == nil && param.Export {
		fmt.Println("导出SQL")
		err = service.Export(param)
	}

	if err != nil {
		fmt.Println("恢复失败:", err)
		log.Fatal("恢复失败:", err)
	}
}
