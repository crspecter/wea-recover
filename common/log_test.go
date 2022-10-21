package common

import (
	"fmt"
	"log"
	"testing"
)

func TestLog(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Llongfile)
	Infoln("aaa", fmt.Errorf("bbb"))
	Errorln("ccc", fmt.Errorf("ddd"))
	err := Error("eee", fmt.Errorf("fff"))
	t.Log(err)
}
