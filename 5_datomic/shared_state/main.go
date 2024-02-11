package main

import (
	"context"
	"encoding/json"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Request struct {
	Transactions [][]any `json:"txn"`
}

func main() {
	node := maelstrom.NewNode()

	kv := maelstrom.NewLinKV(node)

	ctx := context.Background()

	strategy := make(map[string]func(*map[string][]int, []any) []any)

	mainKey := "db"

	fetchState := func() string {
		state, err := kv.Read(ctx, mainKey)
		if err != nil {
			return "{}"
		}
		return state.(string)
	}

	strategy["r"] = func(pointer *map[string][]int, transaction []any) []any {
		key := strconv.Itoa(int(transaction[1].(float64)))

		transactionResponse := make([]any, 0)

		transactionResponse = append(transactionResponse, transaction[0])
		transactionResponse = append(transactionResponse, transaction[1])

		snapshot := *pointer

		value, ok := snapshot[key]

		if ok {
			transactionResponse = append(transactionResponse, value)
		} else {
			transactionResponse = append(transactionResponse, nil)
		}
		return transactionResponse
	}

	strategy["append"] = func(pointer *map[string][]int, transaction []any) []any {
		key := strconv.Itoa(int(transaction[1].(float64)))
		value := int(transaction[2].(float64))

		snapshot := *pointer

		arr, ok := snapshot[key]

		if ok {
			arr = append(arr, value)
		} else {
			arr = make([]int, 0)
			arr = append(arr, value)
		}

		snapshot[key] = arr

		return transaction
	}

	node.Handle("txn", func(msg maelstrom.Message) error {
		var body Request
		json.Unmarshal(msg.Body, &body)

		transactions := make([][]any, 0)

		snapshot := fetchState()

		store := make(map[string][]int)

		json.Unmarshal([]byte(snapshot), &store)

		for _, transaction := range body.Transactions {
			operation := transaction[0].(string)
			txnRes := strategy[operation](&store, transaction)
			transactions = append(transactions, txnRes)
		}

		jsonBytes, _ := json.Marshal(store)

		toSave := string(jsonBytes)

		casErr := kv.CompareAndSwap(ctx, mainKey, snapshot, toSave, true)

		if casErr != nil {
			return node.Reply(msg, map[string]any{
				"type": "error",
				"code": "30",
				"text": casErr.Error(),
			})
		}

		return node.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  transactions,
		})
	})

	node.Run()
}
