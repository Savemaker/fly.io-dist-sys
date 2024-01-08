package main

import (
	"encoding/json"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		json.Unmarshal(msg.Body, &body)
		body["type"] = "generate_ok"
		body["id"] = uuid.New()
		return n.Reply(msg, body)
	})

	n.Run()
}
