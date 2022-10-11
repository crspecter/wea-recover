package parser

import (
	"github.com/go-mysql-org/go-mysql/replication"
)

var EventChan chan (*replication.BinlogEvent)

func WeaFileEventParser(event *replication.BinlogEvent) error {
	EventChan <- event
	return nil
}

func RunParser(file string, offset int64) error {
	parser := replication.NewBinlogParser()
	return parser.ParseFile(file, offset, WeaFileEventParser)
}

func GetFileEvent() *replication.BinlogEvent {
	e := <-EventChan
	return e
}
