package parser

import (
	"context"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func RunDumper(cfg replication.BinlogSyncerConfig, pos mysql.Position) error {
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return err
	}
	for {
		event, err := streamer.GetEvent(context.Background())
		if err != nil {
			return err
		}
		EventChan <- event
	}
}

func DumperGetEvent() *replication.BinlogEvent {
	return <-EventChan
}
