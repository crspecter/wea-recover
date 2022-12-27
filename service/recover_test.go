package service

import (
	"fmt"
	"strconv"
	"testing"
)

func TestDb_GetSchema(t *testing.T) {
	ch := make(chan int, 10)
	for i := 0; i < 6; i++ {
		ch <- i
	}
	close(ch)

	for {
		select {
		case v, ok := <-ch:
			if !ok {
				t.Log("not ok")
				goto END
			}
			t.Log(v)
		}
	}
END:
	t.Log("end")
}

func getNum(v interface{}) interface{} {
	return v
}

func TestParseNumber(t *testing.T) {
	var arr []interface{}
	arr = append(arr, getNum(int(-10)))
	arr = append(arr, getNum(int8(-11)))
	arr = append(arr, getNum(int16(-12)))
	arr = append(arr, getNum(int32(-13)))
	arr = append(arr, getNum(int64(-14)))
	arr = append(arr, getNum(uint(0)))
	arr = append(arr, getNum(uint8(1)))
	arr = append(arr, getNum(uint16(2)))
	arr = append(arr, getNum(uint32(3)))
	arr = append(arr, getNum(uint64(4)))
	for _, v := range arr {
		fmt.Printf("v - %T,%v", v, v)
		switch pkv := v.(type) {
		//case int, int8, int16, int32, int64,
		//	uint, uint8, uint16, uint32, uint64, float32, float64:
		//	nowID = pkv.(int64)
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			nowID, _ := strconv.ParseInt(fmt.Sprintf("%v", pkv), 10, 64)
			fmt.Printf(" nowID - %T,%v\n", nowID, nowID)
		case float32, float64:
			fmt.Printf("float32, float64主键不支持")
		}
	}

}
