package service

import (
	"strings"
	"testing"
	"wea-recover/common/def"
)

func TestDb_Init(t *testing.T) {
	cases := []struct {
		name   string
		param  def.InputInfo
		expect bool
		err    string
	}{{"登录信息错误", def.InputInfo{Addr: "127.0.0.1", Port: 3306, User: "root", Pwd: "error", Db: "testdb", Table: "test1"}, false, "GetConn err"},
		{"成功", def.InputInfo{Addr: "127.0.0.1", Port: 3306, User: "root", Pwd: "123456", Db: "testdb", Table: "test1"}, true, ""}}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			conn := db{}
			err := conn.Init(v.param)
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
