package parser

import "github.com/go-mysql-org/go-mysql/replication"

var EventChan chan (*replication.BinlogEvent)
var EventDone bool = false

type MysqlSyncConfig struct {
	SourceAddr     string
	SourceUser     string
	SourcePassword string
	DBName         string
	TargetAddr     string
	TargetUser     string
	TargetPassword string
	BinlogName     string
	Position       uint32
}
