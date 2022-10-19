package service

import (
	"bytes"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
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
		log.Println("E err:", err)
		return nil
	}

	// 2.构造过滤器
	f := filter{db: param.Db, table: param.Table, stopPosition: param.StopPosition, startPosition: param.StartPosition}
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
	if strings.TrimSpace(param.Binlog) != "" {
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
			Name: param.Binlog,
			Pos:  param.StartPosition,
		}
		binlog = NewDumpParser(cfg, pos)
	} else {
		binlog = NewFileParser(strings.Split(param.BinlogPath, ","), int64(param.StartPosition))
	}
	if binlog == nil {
		log.Println("E 构造binlog parser失败:", err)
		return nil
	}

	file, err := os.OpenFile("raw.sql", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0655)
	if err != nil {
		log.Println("E 打开文件失败:", err)
		return nil
	}

	// 3.返回recover
	return &Recover{
		filter: f,
		conn: db{
			addr:  param.Addr,
			port:  param.Port,
			pwd:   param.Pwd,
			db:    param.Db,
			table: param.Table,
		},
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
			log.Println("I 恢复完成")
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
			sql.ToSql()
			log.Println(sql)
		}
	}

END:
	r.wg.Done()
}

func (r *Recover) recoverData(ev *replication.BinlogEvent) (bool, error) {
	if ev == nil {
		log.Println("I binlog文件列表解析结束")
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
		return nil, fmt.Errorf("binlog rows parse err")
	}
	if len(Rows) == 0 {
		return nil, fmt.Errorf("binlog rows parse err")
	}

	_schema := r.conn.GetSchema()
	if len(_schema.PKColumns) == 0 {
		return nil, fmt.Errorf("no primary key")
	}

	for _, row := range Rows {
		if len(row) != len(_schema.Columns) {
			return nil, fmt.Errorf("binlog rows parse err:colums err")
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
			log.Println("I can not parse WRITE_ROWS_EVENT")
			return nil
		case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			rows, err := r.filterUniqueRow(ev.Rows)
			if err != nil {
				log.Println("E filterUniqueRow err:", err)
				return err
			} else if rows == nil || len(rows) == 0 {
				return nil
			}

			sql, match, err := mysql2.HandleUpdateEvent(rows, r.conn.GetSchema(), mysql2.SqlTypeUpdateOrInsert)
			if err != nil {
				log.Println("E parse update event err:", err)
				return err
			} else if !match || len(sql) == 0 {
				log.Println("update event not match")
				return fmt.Errorf("update event not match")
			}

			for _, v := range sql {
				r.ch <- v
			}
			return nil
		case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			rows, err := r.filterUniqueRow(ev.Rows)
			if err != nil {
				log.Println("E filterUniqueRow err:", err)
				return err
			} else if rows == nil || len(rows) == 0 {
				return nil
			}

			sql, match, err := mysql2.HandleDeleteEvent(rows, r.conn.GetSchema(), mysql2.SqlTypeDelete)
			if err != nil {
				log.Println("E parse delete event err:", err)
				return err
			} else if !match {
				log.Println("update event not match")
				return fmt.Errorf("update event not match")
			}

			r.ch <- sql
			return nil
		default:
			log.Println("I unknown RowsEvent: ", event.Header.EventType)
			return nil
		}
	default:
		log.Println("I unknown event: ", event.Header.EventType)
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
	binlogs  []string
	startPos int64
	curPos   int
}

func NewFileParser(binlogs []string, startPos int64) *fileParser {
	if len(binlogs) < 1 {
		log.Println("E binlogs empty")
		return nil
	}
	err := parser2.RunParser(binlogs[0], startPos)
	if err != nil {
		log.Println("E RunParser err:", err)
		return nil
	}
	return &fileParser{
		binlogs:  binlogs,
		startPos: startPos,
		curPos:   0,
	}
}

func (f *fileParser) GetEvent() *replication.BinlogEvent {
	event := parser2.GetFileEvent()
	if event.Header.Flags == 0xffff {
		f.curPos++
		if len(f.binlogs)-1 < f.curPos {
			log.Println("I input file end")
			return nil
		}
		log.Println("I exchange file:", f.binlogs[f.curPos])
		err := parser2.RunParser(f.binlogs[f.curPos], 0)
		if err != nil {
			log.Println("E RunParser err:", err)
		}
	}
	return parser2.GetFileEvent()
}

type dumpParser struct{}

func NewDumpParser(cfg replication.BinlogSyncerConfig, pos mysql.Position) *dumpParser {
	err := parser2.RunDumper(cfg, pos)
	if err != nil {
		log.Println("E RunDumper err:", err)
		return nil
	}
	return &dumpParser{}
}

func (f *dumpParser) GetEvent() *replication.BinlogEvent {
	return parser2.DumperGetEvent()
}

type db struct {
	addr   string
	port   int
	pwd    string
	db     string
	table  string
	schema *schema.Table
}

func (d *db) GetSchema() *schema.Table {
	return d.schema
}

type filter struct {
	startFile     string
	startPosition uint32
	stopFile      string
	stopPosition  uint32
	startDatetime *time.Time
	stopDatetime  *time.Time
	db            string
	table         string
}

func (f filter) Valid(event *replication.BinlogEvent) bool {
	//只处理RowsQueryEvent,UPDATE_ROWS_EVENTv0与DELETE_ROWS_EVENTv0
	switch ev := event.Event.(type) {
	case *replication.RowsQueryEvent:
		log.Println("I 原始sql:", string(ev.Query))
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
		log.Println("I rotate binlog:", ev.NextLogName, ev.Position)
		return false
	default:
		return false
	}
}

func (f filter) IsFinish(ev *replication.BinlogEvent) bool {
	if f.stopPosition != 0 && ev.Header.LogPos > f.stopPosition {
		log.Println("recover end by stop pos:", f.stopPosition)
		return true
	}
	if f.stopDatetime != nil && int64(ev.Header.Timestamp) > f.stopDatetime.Unix() {
		log.Println("recover end by stop datetime:", f.stopDatetime)
		return true
	}
	return false
}

func check(param def.InputInfo) error {
	return nil
}
