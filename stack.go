package s

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/liuzl/ds"
	"github.com/liuzl/goutil"
	"github.com/liuzl/store"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Stack struct {
	Path string `json:"path"`

	stack        *ds.Stack
	retryQueue   *ds.Queue
	runningStore *store.LevelStore
	exit         chan bool
}

func NewStack(path string) (*Stack, error) {
	s := &Stack{Path: path, exit: make(chan bool)}
	var err error
	stackDir := filepath.Join(path, "stack")
	if s.stack, err = ds.OpenStack(stackDir); err != nil {
		return nil, err
	}
	retryDir := filepath.Join(path, "retry_queue")
	if s.retryQueue, err = ds.OpenQueue(retryDir); err != nil {
		return nil, err
	}
	storeDir := filepath.Join(path, "running")
	if s.runningStore, err = store.NewLevelStore(storeDir); err != nil {
		return nil, err
	}

	go s.retry()

	return s, nil
}

func (s *Stack) Status() map[string]interface{} {
	if s.stack == nil {
		return map[string]interface{}{"error": "stack is nil"}
	}
	return map[string]interface{}{
		"stack_length":       s.stack.Length(),
		"retry_queue_length": s.retryQueue.Length(),
	}
}

func (s *Stack) Push(data string) error {
	if s.stack != nil {
		_, err := s.stack.PushString(data)
		return err
	}
	return fmt.Errorf("stack is nil")
}

func (s *Stack) dequeue(queue *ds.Queue, timeout int64) (string, string, error) {
	item, err := queue.Dequeue()
	if err != nil {
		return "", "", err
	}
	key := ""
	if timeout > 0 {
		now := time.Now().Unix()
		key = goutil.TimeStr(now+timeout) + ":" + goutil.ContentMD5(item.Value)
		if err = s.addToRunning(key, item.Value); err != nil {
			return "", "", err
		}
	}
	return key, string(item.Value), nil
}

func (s *Stack) pop(stack *ds.Stack, timeout int64) (string, string, error) {
	item, err := stack.Pop()
	if err != nil {
		return "", "", err
	}
	key := ""
	if timeout > 0 {
		now := time.Now().Unix()
		key = goutil.TimeStr(now+timeout) + ":" + goutil.ContentMD5(item.Value)
		if err = s.addToRunning(key, item.Value); err != nil {
			return "", "", err
		}
	}
	return key, string(item.Value), nil
}

func (s *Stack) Pop(timeout int64) (string, string, error) {
	if s.retryQueue != nil && s.retryQueue.Length() > 0 {
		return s.dequeue(s.retryQueue, timeout)
	}
	if s.stack != nil && s.stack.Length() > 0 {
		return s.pop(s.stack, timeout)
	}
	return "", "", fmt.Errorf("Stack is empty")
}

func (s *Stack) Confirm(key string) error {
	if s.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return s.runningStore.Delete(key)
}

func (s *Stack) Close() {
	if s.exit != nil {
		s.exit <- true
	}
	if s.stack != nil {
		s.stack.Close()
	}
	if s.retryQueue != nil {
		s.retryQueue.Close()
	}
	if s.runningStore != nil {
		s.runningStore.Close()
	}
}

func (s *Stack) Drop() {
	s.Close()
	os.RemoveAll(s.Path)
}

func (s *Stack) addToRunning(key string, value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("empty value")
	}
	if s.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return s.runningStore.Put(key, value)
}

func (s *Stack) retry() {
	for {
		select {
		case <-s.exit:
			return
		default:
			now := time.Now().Format("20060102030405")
			s.runningStore.ForEach(&util.Range{Limit: []byte(now)},
				func(key, value []byte) (bool, error) {
					if _, err := s.retryQueue.Enqueue(value); err != nil {
						return false, err
					}
					s.runningStore.Delete(string(key))
					return true, nil
				})
			goutil.Sleep(5*time.Second, s.exit)
		}
	}
}
