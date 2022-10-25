package service

import (
	"bytes"
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"wea-recover/common"
	"wea-recover/common/def"
	mysql2 "wea-recover/mysql"
	parser2 "wea-recover/parser"
)

type Recover struct {
	binlog parser
	conn   db
	filter filter
	fd     *os.File
	pkMap  map[string]struct{}
	ch     chan *mysql2.SqlAttr
	wg     sync.WaitGroup
}

// 数据恢复器创建
func NewRecover(param def.InputInfo) *Recover {
	// 1.参数检查
	err := check(param)
	if err != nil {
		common.Errorln(err)
		return nil
	}

	// 2.构造过滤器
	f := filter{db: param.Db, table: param.Table, binlogs: param.Binlogs, ty: param.Ty, isLastBinlog: false}
	if param.StartDatetime == "" {
		f.startDatetime = nil
	} else {
		t, _ := time.Parse("2006-01-02_15:04:05", param.StartDatetime)
		f.startDatetime = &t
	}
	if param.StopDatetime == "" {
		f.stopDatetime = nil
	} else {
		t, _ := time.Parse("2006-01-02_15:04:05", param.StopDatetime)
		f.stopDatetime = &t
	}

	// 3.构造parser
	var binlog parser
	if param.Ty == def.DUMP_RECOVER {
		cfg := replication.BinlogSyncerConfig{
			ServerID:        rand.New(rand.NewSource(time.Now().UnixNano())).Uint32(),
			Host:            param.Addr,
			Port:            uint16(param.Port),
			User:            param.User,
			Password:        param.Pwd,
			RecvBufferSize:  1024 * 1024,
			HeartbeatPeriod: time.Second * 30,
		}
		pos := mysql.Position{
			Name: param.Binlogs[0].Binlog,
			Pos:  param.Binlogs[0].Pos,
		}
		binlog = NewDumpParser(cfg, pos)
	} else if param.Ty == def.FILE_RECOVER {
		binlog = NewFileParser(param.Binlogs)
	}
	if binlog == nil {
		common.Errorln("构造binlog parser失败:", err)
		return nil
	}

	file, err := os.OpenFile("raw.sql", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0655)
	if err != nil {
		common.Errorln("打开文件失败:", err)
		return nil
	}

	//TODO 获取表结构,创建test库, table_recover表,清空表@hao.hu
	conn := db{
		addr:  param.Addr,
		port:  param.Port,
		user:  param.User,
		pwd:   param.Pwd,
		db:    param.Db,
		table: param.Table,
	}
	err = conn.Init()
	if err != nil {
		common.Errorln("数据库初始化失败:", err)
		return nil
	}

	// 3.返回recover
	return &Recover{
		filter: f,
		conn:   conn,
		binlog: binlog,
		fd:     file,
		pkMap:  make(map[string]struct{}),
		ch:     make(chan *mysql2.SqlAttr, 10),
	}
}

// 执行数据恢复
func (r *Recover) Run() error {
	defer func() {
		if r.fd != nil {
			_ = r.fd.Close()
		}
	}()

	go r.write()

	for {
		finish, err := r.recoverData(r.binlog.GetEvent())
		if err != nil {
			close(r.ch)
			r.wg.Wait()
			return err
		}
		if finish {
			close(r.ch)
			r.wg.Wait()
			common.Infoln("恢复完成")
			return nil
		}
	}
}

func (r *Recover) write() {
	r.wg.Add(1)
	for {
		select {
		case sql, ok := <-r.ch:
			if !ok {
				goto END
			}
			//修改写入test库的表名称为xxx_recover
			tName := sql.TName + "_recover"
			sql.TName = tName
			sqlCmd := sql.ToSql()
			//写入
			err := mysql2.ExecuteTestDB(sqlCmd)
			if err != nil {
				common.Errorln("写入恢复库执行sql失败:", err)
				fmt.Println("写入恢复库执行sql失败:", err)
				os.Exit(-1)
			}
			common.Infoln("exec sql:", sqlCmd)
		}
	}

END:
	r.wg.Done()
}

func (r *Recover) recoverData(ev *replication.BinlogEvent) (bool, error) {
	if ev == nil {
		common.Infoln("binlog文件列表解析结束")
		return true, nil
	}

	// 2.判断是否终止
	if r.filter.IsFinish(ev) {
		return true, nil
	}

	// 3.过滤事件
	if !r.filter.Valid(ev) {
		return false, nil
	}

	// 3.解析事件恢复数据
	err := r.parseEvent(ev)
	if err != nil {
		return false, err
	}

	return false, nil
}

func (r *Recover) filterUniqueRow(Rows [][]interface{}) ([][]interface{}, error) {
	if len(Rows)%2 != 0 {
		return nil, common.Error("binlog rows parse err")
	}
	if len(Rows) == 0 {
		return nil, common.Error("binlog rows parse err")
	}

	_schema := r.conn.GetSchema()
	if len(_schema.PKColumns) == 0 {
		return nil, common.Error("no primary key")
	}

	for _, row := range Rows {
		if len(row) != len(_schema.Columns) {
			return nil, common.Error("binlog rows parse err:colums err")
		}
	}

	var ret [][]interface{}
	for _, row := range Rows {
		key := ""
		for _, pkIndex := range _schema.PKColumns {
			str := fmt.Sprintf("%v.", row[pkIndex])
			key += str
		}
		_, ok := r.pkMap[key]
		if !ok {
			ret = append(ret, row)
		}
	}

	return ret, nil
}

func (r *Recover) parseEvent(event *replication.BinlogEvent) error {
	switch ev := event.Event.(type) {
	case *replication.RowsQueryEvent:
		r.exportRawSql(ev.Query)
	case *replication.RowsEvent:
		switch event.Header.EventType {
		case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			common.Infoln("can not parse WRITE_ROWS_EVENT")
			return nil
		case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			rows, err := r.filterUniqueRow(ev.Rows)
			if err != nil {
				return common.Error("filterUniqueRow err:", err)
			} else if rows == nil || len(rows) == 0 {
				return nil
			}

			sql, match, err := mysql2.HandleUpdateEvent(rows, r.conn.GetSchema(), mysql2.SqlTypeUpdateOrInsert)
			if err != nil {
				return common.Error("parse update event err:", err)
			} else if !match || len(sql) == 0 {
				return common.Error("update event not match")
			}

			for _, v := range sql {
				r.ch <- v
			}
			return nil
		case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			rows, err := r.filterUniqueRow(ev.Rows)
			if err != nil {
				return common.Error("filterUniqueRow err:", err)
			} else if rows == nil || len(rows) == 0 {
				return nil
			}

			sql, match, err := mysql2.HandleDeleteEvent(rows, r.conn.GetSchema(), mysql2.SqlTypeDelete)
			if err != nil {
				return common.Error("parse delete event err:", err)
			} else if !match {
				return common.Error("update event not match")
			}

			r.ch <- sql
			return nil
		default:
			common.Infoln("unknown RowsEvent: ", event.Header.EventType)
			return nil
		}
	default:
		common.Infoln("unknown event: ", event.Header.EventType)
	}
	return nil
}

func (r *Recover) exportRawSql(sql []byte) {
	_, _ = r.fd.Write(append(sql, '\n'))
}

type parser interface {
	GetEvent() *replication.BinlogEvent
}

type fileParser struct {
	binlogs []def.BinlogPos
	curPos  int
}

func NewFileParser(binlogs []def.BinlogPos) *fileParser {
	startPos := int64(binlogs[0].Pos)
	if len(binlogs) < 1 {
		common.Errorln("binlogs empty")
		return nil
	}
	err := parser2.RunParser(binlogs[0].Binlog, startPos)
	if err != nil {
		common.Errorln("RunParser err:", err)
		return nil
	}
	return &fileParser{
		binlogs: binlogs,
		curPos:  0,
	}
}

func (f *fileParser) GetEvent() *replication.BinlogEvent {
	event := parser2.GetFileEvent()
	if event.Header.Flags == 0xffff {
		f.curPos++
		if len(f.binlogs)-1 < f.curPos {
			common.Infoln("input file end")
			return nil
		}
		common.Infoln("exchange file:", f.binlogs[f.curPos])
		err := parser2.RunParser(f.binlogs[f.curPos].Binlog, 0)
		if err != nil {
			common.Errorln("RunParser err:", err)
			return nil
		}
	} else if len(f.binlogs)-1 == f.curPos && f.binlogs[f.curPos].Pos != 0 {
		//最后一个binlog,并且设置了截止位点
		if event.Header.LogPos > f.binlogs[f.curPos].Pos {
			common.Infoln("input file end")
			return nil
		}
	}
	return parser2.GetFileEvent()
}

type dumpParser struct{}

func NewDumpParser(cfg replication.BinlogSyncerConfig, pos mysql.Position) *dumpParser {
	err := parser2.RunDumper(cfg, pos)
	if err != nil {
		common.Errorln("RunParser err:", err)
		return nil
	}
	return &dumpParser{}
}

func (f *dumpParser) GetEvent() *replication.BinlogEvent {
	return parser2.DumperGetEvent()
}

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

func (d *db) Init() error {
	//构建链接需要恢复数据库连接池
	mysql2.NewConnPool(mysql2.DBConfig{
		Addr:     d.addr + ":" + strconv.Itoa(d.port),
		User:     d.user,
		Password: d.pwd,
		DBName:   d.db,
	})

	d.clientPool = mysql2.Pool

	// 创建执行sql的连接池test库连接池
	mysql2.NewConnTestPool(mysql2.DBConfig{
		Addr:     d.addr + ":" + strconv.Itoa(d.port),
		User:     d.user,
		Password: d.pwd,
		DBName:   "test",
	})
	d.TestDBPool = mysql2.TestDBPool

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	conn, err := d.TestDBPool.GetConn(ctx)
	if err != nil {
		return common.Error("GetConn err:", err)
	}
	defer d.TestDBPool.PutConn(conn)
	_, err = conn.Execute("CREATE DATABASE IF NOT EXISTS test;")
	if err != nil {
		return common.Error("CREATE DATABASE test err:", err)
	}

	err = d.updateSchema()
	if err != nil {
		return err
	}

	sql, err := d.getShowCreateTableSql(d.table)
	if err != nil {
		return common.Error("getShowCreateTableSql err:", err)
	}

	//创建test表
	sql = strings.Replace(sql, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`", d.table), fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`", d.table+"_recover"), 1)
	var rsl *mysql.Result
	rsl, err = conn.Execute(sql)
	if err != nil {
		return common.Error("执行建表语句执行失败 err:", err)
	}
	rsl.Close()

	sql = fmt.Sprintf("DROP TABLE `%s`", d.table+"_recover")
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
	sql := fmt.Sprintf("show full columns from `%s`.`%s`", d.db, d.table)
	r, err := conn.Execute(sql)
	if err != nil {
		return common.Error("Execute", sql, "err:", err)
	}
	for i := 0; i < r.RowNumber(); i++ {
		extra, _ := r.GetString(i, 6)
		if extra == "STORED GENERATED" {
			t.Columns[i].IsVirtual = true
		}
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

type filter struct {
	ty            def.RecoverType
	binlogs       []def.BinlogPos
	startDatetime *time.Time
	stopDatetime  *time.Time
	db            string
	table         string
	isLastBinlog  bool
}

func (f filter) Valid(event *replication.BinlogEvent) bool {
	//只处理RowsQueryEvent,UPDATE_ROWS_EVENTv0与DELETE_ROWS_EVENTv0
	switch ev := event.Event.(type) {
	case *replication.RowsQueryEvent:
		common.Infoln("原始sql:", string(ev.Query))
		if bytes.Contains(ev.Query, []byte(f.table)) && !bytes.Contains(ev.Query, []byte("select")) {
			return true
		}
		return false
	case *replication.RowsEvent:
		dbName, oTableName := string(ev.Table.Schema), string(ev.Table.Table)
		if dbName != f.db || oTableName != f.table {
			return false
		}
		switch event.Header.EventType {
		case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			return false
		case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			return true
		case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			return true
		default:
			return false
		}
	case *replication.RotateEvent:
		common.Infoln("rotate binlog:", string(ev.NextLogName), ev.Position)
		return false
	default:
		return false
	}
}

func (f *filter) IsFinish(ev *replication.BinlogEvent) bool {
	//if f.stopPosition != 0 && ev.Header.LogPos > f.stopPosition {
	//	common.Infoln("recover end by stop pos:", f.stopPosition)
	//	return true
	//}

	//是否为最后一个binlog
	if f.ty == def.DUMP_RECOVER && len(f.binlogs) == 2 && f.isLastBinlog == false {
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			if string(e.NextLogName) == f.binlogs[1].Binlog {
				f.isLastBinlog = true
				return false
			}
		}
	}

	//最后一个binlog, 对比截止位点
	if f.isLastBinlog {
		_, ok := ev.Event.(*replication.RotateEvent)
		if f.binlogs[1].Pos == 0 && ok {
			return true
		}
		if f.binlogs[1].Pos != 0 && ev.Header.LogPos > f.binlogs[1].Pos {
			return true
		}
	}

	if f.stopDatetime != nil && int64(ev.Header.Timestamp) > f.stopDatetime.Unix() {
		common.Infoln("recover end by stop datetime:", f.stopDatetime)
		return true
	}
	return false
}

func check(param def.InputInfo) error {
	return nil
}
