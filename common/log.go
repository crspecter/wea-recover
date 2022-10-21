package common

import (
	"fmt"
	"log"
)

func Infoln(v ...interface{}) {
	var arr []interface{}
	arr = append(arr, "I ")
	arr = append(arr, v...)
	log.Println(arr...)
}

func Errorln(v ...interface{}) {
	var arr []interface{}
	arr = append(arr, "E ")
	arr = append(arr, v...)
	log.Println(arr...)
}

func Error(v ...interface{}) error {
	var arr []interface{}
	arr = append(arr, "E ")
	arr = append(arr, v...)
	log.Println(arr...)

	return fmt.Errorf(fmt.Sprintln(arr...))
}
