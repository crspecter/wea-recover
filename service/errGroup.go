package service

import (
	"sync"

	"github.com/gogf/gf/container/garray"
)

type ErrorGroup struct {
	locker sync.WaitGroup
	err    *garray.Array
	ch     chan struct{}
}

func (e *ErrorGroup) Add(fn func() error) {
	if e.err.Len() != 0 {
		return
	}
	e.locker.Add(1)
	e.ch <- struct{}{}
	go func() {
		defer func() {
			// 使其让出容量
			<-e.ch
		}()
		defer e.locker.Done()
		if e.err.Len() != 0 {

		} else {
			err := fn()
			if err != nil {
				e.err.Append(err)
			}
		}
	}()
}

func (e *ErrorGroup) Wait() error {
	e.locker.Wait()
	if e.err.Len() != 0 {
		data, ok := e.err.PopRight()
		if ok {
			return data.(error)
		}
	}
	return nil
}

func (e *ErrorGroup) HaveError() bool {
	return e.err.Len() != 0
}

func NewGroup(maxGroup int) *ErrorGroup {
	return &ErrorGroup{
		locker: sync.WaitGroup{},
		err:    garray.New(true),
		ch:     make(chan struct{}, maxGroup),
	}
}
