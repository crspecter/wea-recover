package service

import (
	"strings"
	"testing"
	"wea-recover/common/def"
)

func TestNewFileParser(t *testing.T) {
	cases := []struct {
		name   string
		param  []def.BinlogPos
		expect bool
		err    string
	}{{"binlog为空", []def.BinlogPos{}, false, "binlogs empty"},
		{"binlog不存在", []def.BinlogPos{{Binlog: "/path/file", Pos: 0}}, false, "RunParser err"},
		{"binlog位点超出范围", []def.BinlogPos{{Binlog: "C:\\Program Files\\MySQL\\MySQL Server 8.0\\data\\binlog.000040", Pos: 0xffff}}, true, ""}}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			_, err := NewFileParser(v.param)
			if (v.expect && err != nil) ||
				(err == nil && v.expect == false) {
				t.Error("expect:", v.expect, "but get err:", err)
				return
			} else if err != nil && v.expect == false {
				if strings.Contains(err.Error(), v.err) {
					t.Log("log:", err)
					return
				} else {
					t.Error("expect err:", v.err, "but get err:", err)
					return
				}
			}
		})
	}
}
