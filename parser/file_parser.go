package parser

import (
	"github.com/go-mysql-org/go-mysql/replication"
)

func WeaFileEventParser(event *replication.BinlogEvent) error {
	EventChan <- event
	return nil
}

func RunParser(file string, offset int64) error {
	parser := replication.NewBinlogParser()
	err := parser.ParseFile(file, offset, WeaFileEventParser)
	if err != nil {
		EventDone = true
		return err
	} else {
		EventDone = true
		return nil
	}
}

func GetFileEvent() *replication.BinlogEvent {
	if EventDone {
		return nil
	} else {
		e := <-EventChan
		return e
	}
}
