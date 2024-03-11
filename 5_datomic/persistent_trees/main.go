package main

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Request struct {
	Transactions [][]any `json:"txn"`
}

func main() {
	node := maelstrom.NewNode()

	store := maelstrom.NewLinKV(node)

	generatorLock := sync.Mutex{}

	atomicID := 0

	generateID := func(generatorLock *sync.Mutex) string {
		generatorLock.Lock()
		defer generatorLock.Unlock()
		atomicID += 1
		return node.ID() + strconv.Itoa(atomicID)
	}

	node.Handle("txn", func(msg maelstrom.Message) error {
		var currentDBPointer string

		if currentDBPointerData, err := store.Read(context.Background(), "db"); err == nil {
			currentDBPointer = currentDBPointerData.(string)
		}

		newDBPointer := generateID(&generatorLock)

		newDBValue := make(map[int]string)

		if currentDBPointer != "" {
			store.ReadInto(context.Background(), currentDBPointer, &newDBValue)
		}

		var body Request
		json.Unmarshal(msg.Body, &body)

		transactionResponses := make([][]any, 0)

		for _, transaction := range body.Transactions {
			operation := transaction[0].(string)
			currentKey := int(transaction[1].(float64))

			if operation == "r" {
				transactionResponse := make([]any, 0)
				transactionResponse = append(transactionResponse, transaction[0:2]...)

				val, ok := newDBValue[currentKey]
				if ok {
					var actualValues []int
					if err := store.ReadInto(context.Background(), val, &actualValues); err == nil {
						transactionResponse = append(transactionResponse, actualValues)
					}
				} else {
					transactionResponse = append(transactionResponse, nil)
				}
				transactionResponses = append(transactionResponses, transactionResponse)

			} else if operation == "append" {
				valueToAppend := int(transaction[2].(float64))
				transactionResponses = append(transactionResponses, transaction)

				newValuePointer := generateID(&generatorLock)

				oldValuePointer, ok := newDBValue[currentKey]
				if ok {
					var oldValues []int
					store.ReadInto(context.Background(), oldValuePointer, &oldValues)

					updatedValues := append(oldValues, valueToAppend)
					store.Write(context.Background(), newValuePointer, updatedValues)
				} else {
					newValue := make([]int, 0)
					newValue = append(newValue, valueToAppend)
					store.Write(context.Background(), newValuePointer, newValue)
				}
				newDBValue[currentKey] = newValuePointer
			}
		}

		store.Write(context.Background(), newDBPointer, newDBValue)

		if err := store.CompareAndSwap(context.Background(), "db", currentDBPointer, newDBPointer, true); err != nil {
			return node.Reply(msg, map[string]any{
				"type": "error",
				"code": "30",
				"text": err.Error(),
			})
		} else {
			return node.Reply(msg, map[string]any{
				"type": "txn_ok",
				"txn":  transactionResponses,
			})
		}
	})

	node.Run()
}
