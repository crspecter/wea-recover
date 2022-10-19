package service

import "testing"

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
