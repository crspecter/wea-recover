package def

type InputInfo struct {
	Addr          string
	Port          int
	Pwd           string
	Db            string
	Table         string
	Binlog        string
	BinlogPath    string
	StartDatetime string
	StopDatetime  string
	StartPosition int64
	StopPosition  int64
	Export        bool
}
