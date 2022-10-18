package service

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"log"
	"math/rand"
	"strings"
	"time"
	"wea-recover/common/def"
	parser2 "wea-recover/parser"
)

type Recover struct {
	binlog parser
	conn   db
	filter filter
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
	f := filter{}
	f.stopPosition = param.StopPosition
	f.startPosition = param.StartPosition
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
	}
}

// 执行数据恢复
func (r *Recover) Run() error {
	for {
		err := r.recoverData(r.binlog.GetEvent())
		if err != nil {
			return err
		}
	}
}

func (r *Recover) recoverData(ev *replication.BinlogEvent) error {
	if ev == nil {
		return fmt.Errorf("解析binlog事件失败")
	}

	// 2.判断是否终止
	if r.filter.stopPosition != 0 && ev.Header.LogPos > r.filter.stopPosition {
		log.Println("recover end by stop pos:", r.filter.stopPosition)
		return nil
	}
	if r.filter.stopDatetime != nil && int64(ev.Header.Timestamp) > r.filter.stopDatetime.Unix() {
		log.Println("recover end by stop datetime:", r.filter.stopDatetime)
		return nil
	}

	// 3.解析事件(过滤事件, 恢复数据)
	err := r.parseEvent(ev)
	if err != nil {
		return err
	}

	return nil
}

func (r *Recover) parseEvent(event *replication.BinlogEvent) error {
	if !r.filter.Valid(event) {
		return nil
	}

	switch ev := event.Event.(type) {
	case *replication.RowsQueryEvent:
		fmt.Println("原始sql:", string(ev.Query))
	case *replication.RowsEvent:
		//todo:待恢复数据入库
	}
	return nil
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
	switch event.Event.(type) {
	case *replication.RotateEvent:
		f.curPos++
		if len(f.binlogs)-1 < f.curPos {
			log.Println("I file end")
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
	addr  string
	port  int
	pwd   string
	db    string
	table string
}

type filter struct {
	startFile     string
	startPosition uint32
	stopFile      string
	stopPosition  uint32
	startDatetime *time.Time
	stopDatetime  *time.Time
}

func (f filter) Valid(event *replication.BinlogEvent) bool {
	return true
}

func check(param def.InputInfo) error {
	return nil
}
