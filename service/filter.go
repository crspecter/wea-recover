package service

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"wea-recover/common"
	"wea-recover/common/def"

	"github.com/go-mysql-org/go-mysql/replication"
	pingcapparser "github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

type filter struct {
	ty            def.RecoverType
	binlogs       []def.BinlogPos
	startDatetime *time.Time
	stopDatetime  *time.Time
	db            string
	table         string
	isLastBinlog  bool
	eventFilter   string
}

var globalCurrentBinlog = ""

func (f *filter) Init(param def.InputInfo) error {
	f.db = param.Db
	f.table = param.Table
	f.binlogs = param.Binlogs
	f.ty = param.Ty
	f.isLastBinlog = false
	f.eventFilter = param.EventFilter

	if param.StartDatetime == "" {
		f.startDatetime = nil
	} else {
		t, _ := time.ParseInLocation("2006-01-02 15:04:05", param.StartDatetime, time.Local)
		f.startDatetime = &t
	}
	if param.StopDatetime == "" {
		f.stopDatetime = nil
	} else {
		t, _ := time.ParseInLocation("2006-01-02 15:04:05", param.StopDatetime, time.Local)
		f.stopDatetime = &t
	}

	globalCurrentBinlog = filepath.Base(param.Binlogs[0].Binlog)
	return nil
}

func (f filter) Valid(event *replication.BinlogEvent) bool {
	if f.startDatetime != nil {
		startTimestamp := f.startDatetime.Unix()
		if int64(event.Header.Timestamp) < startTimestamp {
			return false
		}
	}

	//只处理RowsQueryEvent,UPDATE_ROWS_EVENTv0与DELETE_ROWS_EVENTv0+InsertStmt
	switch ev := event.Event.(type) {
	case *replication.RowsQueryEvent:
		//TODO:待测试,复杂sql时是否能正常工作
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
				if f.eventFilter != "both" && f.eventFilter != "update" {
					return false
				}

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
				if f.eventFilter != "both" && f.eventFilter != "delete" {
					return false
				}

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
			case *ast.InsertStmt:
				if f.eventFilter == "both" {
					return false
				}

				_ = v.Table.Restore(&format.RestoreCtx{
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
			if f.eventFilter != "both" && f.eventFilter != "update" {
				return false
			}
			return true
		case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			if f.eventFilter != "both" && f.eventFilter != "delete" {
				return false
			}
			return true
		default:
			return false
		}
	case *replication.RotateEvent:
		globalCurrentBinlog = string(ev.NextLogName)
		fmt.Println("rotate binlog:", string(ev.NextLogName), ev.Position)
		common.Infoln("rotate binlog:", string(ev.NextLogName), ev.Position)
		return false
	default:
		return false
	}
}

func (f *filter) IsFinish(ev *replication.BinlogEvent) bool {
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
		e, ok := ev.Event.(*replication.RotateEvent)
		if f.binlogs[1].Pos == 0 && ok && string(e.NextLogName) != f.binlogs[1].Binlog {
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
