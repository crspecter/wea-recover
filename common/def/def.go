package def

type InputInfo struct {
	Addr          string
	Port          int
	User          string
	Pwd           string
	Db            string
	Table         string
	Binlog        string
	BinlogPath    string
	StartDatetime string
	StopDatetime  string
	StartPosition uint32
	StopPosition  uint32
	Export        bool
}
