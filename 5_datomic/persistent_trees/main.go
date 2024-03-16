package main

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Request struct {
	Transactions [][]any `json:"txn"`
}

func main() {
	node := maelstrom.NewNode()

	store := maelstrom.NewLWWKV(node)

	ctx := context.Background()

	generatorLock := sync.Mutex{}

	atomicID := 0

	generateID := func(generatorLock *sync.Mutex) string {
		generatorLock.Lock()
		defer generatorLock.Unlock()
		atomicID += 1
		return node.ID() + strconv.Itoa(atomicID)
	}

	initFinished := false

	node.Handle("txn", func(msg maelstrom.Message) error {
		if node.ID() == "n0" && !initFinished {
			store.Write(ctx, "db", "")
			initFinished = true
		}

		currentDBPointer := new(string)

		for {
			if err, backoff := store.ReadInto(ctx, "db", currentDBPointer), 1; err != nil {
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff *= 2
			} else {
				break
			}
		}

		currentDBPointerVal := *currentDBPointer

		newDBPointer := generateID(&generatorLock)

		newDBValue := make(map[int]string)

		if currentDBPointerVal != "" {
			for {
				if err, backoff := store.ReadInto(ctx, currentDBPointerVal, &newDBValue), 1; err != nil {
					time.Sleep(time.Duration(backoff) * time.Millisecond)
					backoff *= 2
				} else {
					break
				}
			}
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
					for {
						if err, backoff := store.ReadInto(ctx, val, &actualValues), 1; err != nil {
							time.Sleep(time.Duration(backoff) * time.Millisecond)
							backoff *= 2
						} else {
							break
						}
					}
					transactionResponse = append(transactionResponse, actualValues)
				} else {
					transactionResponse = append(transactionResponse, nil)
				}
				transactionResponses = append(transactionResponses, transactionResponse)

			} else {
				valueToAppend := int(transaction[2].(float64))
				transactionResponses = append(transactionResponses, transaction)

				newValuePointer := generateID(&generatorLock)

				oldValuePointer, ok := newDBValue[currentKey]
				if ok {
					var oldValues []int

					for {
						if err, backoff := store.ReadInto(ctx, oldValuePointer, &oldValues), 1; err != nil {
							time.Sleep(time.Duration(backoff) * time.Millisecond)
							backoff *= 2
						} else {
							break
						}
					}

					updatedValues := append(oldValues, valueToAppend)
					store.Write(ctx, newValuePointer, updatedValues)

				} else {
					newValue := make([]int, 0)
					newValue = append(newValue, valueToAppend)
					store.Write(ctx, newValuePointer, newValue)
				}
				newDBValue[currentKey] = newValuePointer
			}
		}

		store.Write(ctx, newDBPointer, newDBValue)
		for {
			if err, backoff := store.CompareAndSwap(ctx, "db", currentDBPointerVal, newDBPointer, true), 1; err != nil {
				switch err := err.(type) {
				case *maelstrom.RPCError:
					if err.Code == 22 {
						return node.Reply(msg, map[string]any{
							"type": "error",
							"code": err.Code,
							"text": err.Error(),
						})
					} else {
						time.Sleep(time.Duration(backoff) * time.Millisecond)
						backoff *= 2
					}
				}
			} else {
				break
			}
		}

		return node.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  transactionResponses,
		})
	})

	node.Run()
}
