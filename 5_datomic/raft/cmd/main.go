package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/savemaker/raft/handler"
)

func main() {
	node := maelstrom.NewNode()

	nodeHandler := handler.NewRaftHandler(node)

	node.Handle("read", func(msg maelstrom.Message) error {
		go nodeHandler.Read(&msg)
		return nil
	})

	node.Handle("write", func(msg maelstrom.Message) error {
		go nodeHandler.Write(&msg)
		return nil
	})

	node.Handle("cas", func(msg maelstrom.Message) error {
		go nodeHandler.CaS(&msg)
		return nil
	})

	node.Run()
}
