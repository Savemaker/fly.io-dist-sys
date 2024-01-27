package main

import (
	"context"
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddRequest struct {
	Type  string
	Delta int
}

func main() {
	ctx := context.Background()

	node := maelstrom.NewNode()

	kv := maelstrom.NewSeqKV(node)

	node.Handle("read", func(msg maelstrom.Message) error {
		sum := 0
		for _, id := range node.NodeIDs() {
			val, err := kv.ReadInt(ctx, id)
			if err != nil {
				val = 0
			}
			sum += val
		}
		return node.Reply(msg, map[string]any{"type": "read_ok", "value": sum})
	})

	node.Handle("add", func(msg maelstrom.Message) error {
		var body AddRequest
		json.Unmarshal(msg.Body, &body)

		val, err := kv.ReadInt(ctx, node.ID())
		if err != nil {
			val = 0
		}
		kv.Write(ctx, node.ID(), body.Delta+val)
		return node.Reply(msg, map[string]string{"type": "add_ok"})
	})

	node.Run()
}
