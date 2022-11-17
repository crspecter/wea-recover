package def

type InputInfo struct {
	Addr          string
	Port          int
	User          string
	Pwd           string
	Db            string
	Table         string
	Binlogs       []BinlogPos
	StartDatetime string
	StopDatetime  string
	Export        bool
	Ty            RecoverType
	EventFilter   string
	PageSize      int
}

type RecoverType int

const (
	UNKNOWN      RecoverType = -1
	FILE_RECOVER RecoverType = 0
	DUMP_RECOVER RecoverType = 1
	EXPORT_ONLY  RecoverType = 2
)

type BinlogPos struct {
	Binlog string
	Pos    uint32
}
