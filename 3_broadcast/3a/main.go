package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	db := make([]int, 0)

	n.Handle("topology", func(msg maelstrom.Message) error {
		res := make(map[string]any)
		res["type"] = "topology_ok"
		return n.Reply(msg, res)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		res := make(map[string]any)
		res["type"] = "read_ok"
		res["messages"] = db
		return n.Reply(msg, res)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		json.Unmarshal(msg.Body, &body)
		db = append(db, int(body["message"].(float64)))
		res := make(map[string]any)
		res["type"] = "broadcast_ok"
		return n.Reply(msg, res)
	})

	n.Run()
}
