package main

import (
	"fmt"
	"golang.org/x/term"
	"log"
	"os"
	"syscall"
	"time"
	"wea-recover/common"
)

// 用于表示自己的版本信息
var CommitId string
var Branch string
var Message string

func main() {
	now := time.Now()
	common.Infoln("param:", os.Args)
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

func getPwd() (string, error) {
	//bPwd := false
	//for _, v := range os.Args {
	//	if v == "-p" || v == "--pwd" {
	//		bPwd = true
	//		break
	//	}
	//}
	//if bPwd == false {
	//	return "", fmt.Errorf("请输入-p/--pwd")
	//}

	fmt.Print("Enter Password: ")
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Print("\n")
	password := string(bytePassword)
	return password, err
}
