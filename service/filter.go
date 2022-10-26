package service

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	pingcapparser "github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"strings"
	"time"
	"wea-recover/common"
	"wea-recover/common/def"
)

type filter struct {
	ty            def.RecoverType
	binlogs       []def.BinlogPos
	startDatetime *time.Time
	stopDatetime  *time.Time
	db            string
	table         string
	isLastBinlog  bool
}

func (f *filter) Init(param def.InputInfo) error {
	f.db = param.Db
	f.table = param.Table
	f.binlogs = param.Binlogs
	f.ty = param.Ty
	f.isLastBinlog = false

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
	return nil
}

type SqlFeature struct {
	b []byte
}

func (m *SqlFeature) Write(p []byte) (n int, err error) {
	m.b = append(m.b, p...)
	return len(p), nil
}

func (m *SqlFeature) ToString() string {
	return string(m.b)
}

func (f filter) Valid(event *replication.BinlogEvent) bool {
	//只处理RowsQueryEvent,UPDATE_ROWS_EVENTv0与DELETE_ROWS_EVENTv0
	switch ev := event.Event.(type) {
	case *replication.RowsQueryEvent:
		pr := pingcapparser.New()
		nodes, _, err := pr.Parse(string(ev.Query), "", "")
		if err != nil {
			common.Errorln("parse RowsQueryEvent err:", err)
			return false
		}

		for _, node := range nodes {
			sql := &SqlFeature{}
			switch v := node.(type) {
			case *ast.UpdateStmt:
				_ = v.TableRefs.Restore(&format.RestoreCtx{
					Flags: format.DefaultRestoreFlags,
					In:    sql,
				})
				arr := strings.Split(sql.ToString(), ".")
				if len(arr) == 1 {
					if arr[0] == fmt.Sprintf("`%s`", f.table) {
						common.Infoln("原始sql:", string(ev.Query))
						return true
					}
				} else if len(arr) == 2 {
					if arr[1] == fmt.Sprintf("`%s`", f.table) {
						common.Infoln("原始sql:", string(ev.Query))
						return true
					}
				} else {
					return false
				}
			case *ast.DeleteStmt:
				_ = v.TableRefs.Restore(&format.RestoreCtx{
					Flags: format.DefaultRestoreFlags,
					In:    sql,
				})
				arr := strings.Split(sql.ToString(), ".")
				if len(arr) == 1 {
					if arr[0] == fmt.Sprintf("`%s`", f.table) {
						common.Infoln("原始sql:", string(ev.Query))
						return true
					}
				} else if len(arr) == 2 {
					if arr[1] == fmt.Sprintf("`%s`", f.table) {
						common.Infoln("原始sql:", string(ev.Query))
						return true
					}
				} else {
					return false
				}
			}
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
