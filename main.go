package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"wea-recover/common"
)

// 用于表示自己的版本信息
var CommitId string
var Branch string
var Message string

func main() {
	now := time.Now()
	common.Infoln(os.Args)
	common.Infoln("version:", Branch, CommitId, Message)

	param, err := parseParam()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	run(param)
	common.Infoln("恢复完成, 耗时:", time.Now().Sub(now))
	fmt.Println("恢复完成, 耗时:", time.Now().Sub(now))
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	logFile, err := os.OpenFile("./wea.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Panic("打开日志文件异常")
	}
	log.SetOutput(logFile)
}
