package parser

import (
	"github.com/go-mysql-org/go-mysql/replication"
)

func WeaFileEventParser(event *replication.BinlogEvent) error {
	FileEventChan <- event
	return nil
}

func RunParser(file string, offset int64, parser *replication.BinlogParser) error {
	EventDone = false
	FileEventChan = make(chan *replication.BinlogEvent, 5)
	err := parser.ParseFile(file, offset, WeaFileEventParser)
	if err != nil {
		close(FileEventChan)
		EventDone = true
		return err
	} else {
		close(FileEventChan)
		EventDone = true
		return nil
	}
}

func GetFileEvent() *replication.BinlogEvent {
	e, ok := <-FileEventChan
	if !ok {
		event := &replication.BinlogEvent{
			Header: &replication.EventHeader{
				Flags: 0xffff,
			},
		}
		return event
	} else {
		return e
	}
	//if EventDone {
	//	event := &replication.BinlogEvent{
	//		Header: &replication.EventHeader{
	//			Flags: 0xffff,
	//		},
	//	}
	//	return event
	//} else {
	//	e := <-FileEventChan
	//	return e
	//}
}
