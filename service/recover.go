package service

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"wea-recover/common"
	"wea-recover/common/def"
	mysql2 "wea-recover/mysql"
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
	common.Infoln("check param")
	err := check(param)
	if err != nil {
		common.Errorln(err)
		return nil
	}

	// 2.构造过滤器
	common.Infoln("new filter")
	f := filter{}
	_ = f.Init(param)

	// 3.构造parser
	binlog, err := newBinlogParser(param)
	if err != nil {
		common.Errorln("构造binlog parser失败:", err)
		return nil
	}

	//4. 构造数据库连接
	common.Infoln("new mysql conn")
	conn := db{}
	err = conn.Init(param)
	if err != nil {
		return nil
	}

	//5. 构造原始sql文件句柄
	common.Infoln("new raw sql export fd")
	file, err := os.OpenFile("raw.sql", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		common.Errorln("打开文件[raw.sql]失败:", err)
		return nil
	}

	// 6.返回recover
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

	// sql写test库
	go r.write()

	// 获取event,拼接为sql
	var err error
	for {
		var event *replication.BinlogEvent
		event, err = r.binlog.GetEvent()
		if err != nil {
			if strings.Contains(err.Error(), "context deadline exceeded") {
				common.Infoln("dump模式,获取事件超时,主动结束")
				fmt.Println("dump模式,获取事件超时,主动结束")
				err = nil
			}
			break
		}

		var finish bool
		finish, err = r.recoverData(event)
		if err != nil || finish {
			break
		}
	}

	close(r.ch)
	r.wg.Wait()
	return err
}

func (r *Recover) write() {
	r.wg.Add(1)
	count := 0

	go func() {
		ticker := time.NewTicker(time.Second * 30)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Println("已恢复", count, "条数据...")
			}
		}
	}()

	for {
		select {
		case sql, ok := <-r.ch:
			if !ok {
				goto END
			}
			//修改写入test库的表名称为xxx_recover
			sql.TName += "_recover"
			sqlCmd := sql.ToSql()
			//写入
			err := mysql2.ExecuteTestDB(sqlCmd)
			if err != nil {
				common.Errorln("写入恢复库执行sql失败:", err)
				fmt.Println("写入恢复库执行sql失败:", err)
				os.Exit(-1)
			}
			count += len(sql.Value)
			//common.Infoln("exec sql:", sqlCmd)
		}
	}

END:
	fmt.Println("总共恢复", count, "条数据")
	common.Infoln("总共恢复", count, "条数据")
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

func (r *Recover) filterUniqueRow(Rows [][]interface{}, ty eventType) ([][]interface{}, error) {
	if len(Rows) == 0 {
		return nil, common.Error("binlog rows parse err")
	}
	if ty == UPDATE_ROWS && len(Rows)%2 != 0 {
		return nil, common.Error("update binlog rows parse err")
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
	if ty == DELETE_ROWS {
		for _, row := range Rows {
			key := ""
			for _, pkIndex := range _schema.PKColumns {
				str := fmt.Sprintf("%v.", row[pkIndex])
				key += str
			}
			_, ok := r.pkMap[key]
			if !ok {
				ret = append(ret, row)
				r.pkMap[key] = struct{}{}
			}
		}
	} else if ty == UPDATE_ROWS {
		for i := 0; i < len(Rows); i += 2 {
			row := Rows[i]
			key := ""
			for _, pkIndex := range _schema.PKColumns {
				str := fmt.Sprintf("%v.", row[pkIndex])
				key += str
			}
			_, ok := r.pkMap[key]
			if !ok {
				ret = append(ret, row, Rows[i+1])
				r.pkMap[key] = struct{}{}
			}
		}
	}

	return ret, nil
}

type eventType int

const (
	UPDATE_ROWS eventType = iota
	DELETE_ROWS
)

func (r *Recover) parseEvent(event *replication.BinlogEvent) error {
	switch ev := event.Event.(type) {
	case *replication.RowsQueryEvent:
		r.exportRawSql(ev.Query, event.Header)
	case *replication.RowsEvent:
		switch event.Header.EventType {
		case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			rows, err := r.filterUniqueRow(ev.Rows, UPDATE_ROWS)
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
			rows, err := r.filterUniqueRow(ev.Rows, DELETE_ROWS)
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

func (r *Recover) exportRawSql(sql []byte, header *replication.EventHeader) {
	t := time.Unix(int64(header.Timestamp), 0)
	strTime := t.Format("2006-01-02 15:04:05")
	pos := globalCurrentBinlog + " " + strconv.Itoa(int(header.LogPos-header.EventSize))
	info := strTime + " " + pos + " " + string(sql)
	_, _ = r.fd.Write(append([]byte(info), '\n'))
}

func check(param def.InputInfo) error {
	return nil
}
