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

		num := int(body["message"].(float64))

		if isNewBroadcast(num, db) {
			db = append(db, num)
			for _, nid := range n.NodeIDs() {
				n.Send(nid, body)
			}
		}
		res := make(map[string]any)
		res["type"] = "broadcast_ok"
		return n.Reply(msg, res)
	})

	n.Run()
}

func isNewBroadcast(num int, db []int) bool {
	for _, n := range db {
		if n == num {
			return false
		}
	}
	return true
}
