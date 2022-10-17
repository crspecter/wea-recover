package service

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"log"
	"time"
	"wea-recover/common/def"
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
		log.Println("err:", err)
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
	return nil
}

func (r *Recover) recoverData(ev *replication.BinlogEvent) error {
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
}

type dumpParser struct {
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
