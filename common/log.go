package common

import (
	"fmt"
	"log"
)

func Infoln(v ...interface{}) {
	var arr []interface{}
	arr = append(arr, "I ")
	arr = append(arr, v...)
	log.Output(2, fmt.Sprintln(arr...))
	//log.Println(arr...)
}

func Errorln(v ...interface{}) {
	var arr []interface{}
	arr = append(arr, "E ")
	arr = append(arr, v...)
	log.Output(2, fmt.Sprintln(arr...))
	//log.Println(arr...)
}

func Error(v ...interface{}) error {
	var arr []interface{}
	arr = append(arr, "E ")
	arr = append(arr, v...)
	//log.Println(arr...)
	log.Output(2, fmt.Sprintln(arr...))
	return fmt.Errorf(fmt.Sprintln(arr...))
}
