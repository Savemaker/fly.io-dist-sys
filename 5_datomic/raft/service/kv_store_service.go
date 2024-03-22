package service

import (
	"errors"
	"sync"
)

const (
	ERR_NOT_FOUND = "key not found"
	ERR_CAS       = "from has changed"
)

type KeyValueStoreService interface {
	Read(ReadRequestBody) (ReadResponseBody, error)
	Write(WriteRequestBody) (WriteResponseBody, error)
	CaS(CaSRequestBody) (CaSResponseBody, error)
}

type KVStoreService struct {
	store map[int]int
	lock  sync.Mutex
}

func NewKVStoreService() *KVStoreService {
	return &KVStoreService{
		store: make(map[int]int),
		lock:  sync.Mutex{},
	}
}

type ReadRequestBody struct {
	Key int `json:"key"`
}

type ReadResponseBody struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func (service *KVStoreService) Read(request ReadRequestBody) (ReadResponseBody, error) {
	service.lock.Lock()
	val, ok := service.store[request.Key]
	service.lock.Unlock()
	if ok {
		return ReadResponseBody{Value: val, Type: "read_ok"}, nil
	} else {
		return ReadResponseBody{}, errors.New(ERR_NOT_FOUND)
	}
}

type WriteRequestBody struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

type WriteResponseBody struct {
	Type string `json:"type"`
}

func (service *KVStoreService) Write(request WriteRequestBody) (WriteResponseBody, error) {
	service.lock.Lock()
	service.store[request.Key] = request.Value
	service.lock.Unlock()
	return WriteResponseBody{Type: "write_ok"}, nil
}

type CaSRequestBody struct {
	Key  int `json:"key"`
	From int `json:"from"`
	To   int `json:"to"`
}

type CaSResponseBody struct {
	Type string `json:"type"`
}

func (service *KVStoreService) CaS(request CaSRequestBody) (CaSResponseBody, error) {
	service.lock.Lock()
	defer service.lock.Unlock()
	val, ok := service.store[request.Key]
	if ok {
		if val == request.From {
			service.store[request.Key] = request.To
			return CaSResponseBody{Type: "cas_ok"}, nil
		} else {
			return CaSResponseBody{}, errors.New(ERR_CAS)
		}
	} else {
		return CaSResponseBody{}, errors.New(ERR_NOT_FOUND)
	}
}
