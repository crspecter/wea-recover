package service

import (
	"context"
	"math/rand"
	"path/filepath"
	"time"
	"wea-recover/common"
	"wea-recover/common/def"
	parser2 "wea-recover/parser"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type parser interface {
	GetEvent() (*replication.BinlogEvent, error)
}

func newBinlogParser(param def.InputInfo) (parser, error) {
	var binlog parser
	var err error
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
		binlog, err = NewDumpParser(cfg, pos)
	} else if param.Ty == def.FILE_RECOVER {
		binlog, err = NewFileParser(param.Binlogs)
	}

	return binlog, err
}

type fileParser struct {
	binlogs []def.BinlogPos
	curPos  int
	parser  *replication.BinlogParser
}

func NewFileParser(binlogs []def.BinlogPos) (*fileParser, error) {
	if len(binlogs) < 1 {
		return nil, common.Error("binlogs empty")
	}
	common.Infoln("new file parser:", binlogs)

	ret := &fileParser{
		binlogs: binlogs,
		curPos:  0,
		parser:  replication.NewBinlogParser(),
	}

	err := ret.Init(ret.binlogs[0].Binlog, int64(ret.binlogs[0].Pos))
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (f *fileParser) Init(file string, offset int64) error {
	done := false
	var err error = nil
	go func() {
		common.Infoln("RunParser start:", file, offset)
		err = parser2.RunParser(file, offset, f.parser)
		done = true
		if err != nil {
			err = common.Error("RunParser err:", err)
		} else {
			common.Infoln("RunParser done:", file, offset)
		}
	}()

	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		if done {
			return err
		}
	}

	return err
}

func (f *fileParser) GetEvent() (*replication.BinlogEvent, error) {
	event := parser2.GetFileEvent()
	if event.Header.Flags == 0xffff {
		f.curPos++
		if len(f.binlogs)-1 < f.curPos {
			common.Infoln("input file end")
			return nil, nil
		}

		if (len(f.binlogs)-1 == f.curPos) && (f.binlogs[f.curPos].Binlog == f.binlogs[0].Binlog) {
			common.Infoln("input file end")
			return nil, nil
		}

		common.Infoln("exchange file:", f.binlogs[f.curPos])
		globalCurrentBinlog = filepath.Base(f.binlogs[f.curPos].Binlog)
		err := f.Init(f.binlogs[f.curPos].Binlog, 0)
		if err != nil {
			common.Errorln("RunParser err:", err)
			return nil, common.Error("RunParser err:", err)
		}
		return parser2.GetFileEvent(), nil
	} else if len(f.binlogs)-1 == f.curPos && f.binlogs[f.curPos].Pos != 0 && event.Header.LogPos > f.binlogs[f.curPos].Pos {
		//最后一个binlog,并且设置了截止位点
		common.Infoln("input file end")
		return nil, nil
	} else if len(f.binlogs) == 2 && f.binlogs[0].Binlog == f.binlogs[1].Binlog && event.Header.LogPos > f.binlogs[1].Pos {
		//起止的文件是一个，只是位点不同
		common.Infoln("input file end ")
		return nil, nil
	} else {
		return event, nil
	}
}

type dumpParser struct {
	streamer *replication.BinlogStreamer
	err      error
}

func NewDumpParser(cfg replication.BinlogSyncerConfig, pos mysql.Position) (*dumpParser, error) {
	common.Infoln("new dump parser:", cfg, pos)

	syncer := replication.NewBinlogSyncer(cfg)
	if syncer == nil {
		return nil, common.Error("NewBinlogSyncer get syncer == nil")
	}

	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return nil, common.Error("start sync err:", err)
	}
	return &dumpParser{streamer: streamer}, nil
}

func (f *dumpParser) GetEvent() (*replication.BinlogEvent, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*20)
	defer cancelFunc()
	event, err := f.streamer.GetEvent(ctx)
	if err != nil {
		common.Errorln("GetEvent err:", err)
		return nil, err
	}
	return event, nil
}
