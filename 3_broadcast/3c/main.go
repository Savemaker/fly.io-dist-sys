package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SyncMessage struct {
	Type string
	Db   []int
}

type TopologyMessage struct {
	Type     string
	Topology map[string][]string
}

type BroadcastMessage struct {
	Type    string
	Message int
}

type Responder struct {
	Node         *maelstrom.Node
	Msg          maelstrom.Message
	ResponseType string
	Db           *map[int]bool
}

func main() {
	node := maelstrom.NewNode()

	db := make(map[int]bool)
	dbMutex := sync.Mutex{}

	nodeTopology := make([]string, 0)

	mapToSlice := func() []int {
		dbMutex.Lock()
		defer dbMutex.Unlock()
		slice := make([]int, len(db))
		i := 0
		for k := range db {
			slice[i] = k
			i = i + 1
		}
		return slice
	}

	replyOk := func(responseType string, msg maelstrom.Message) {
		response := make(map[string]any)

		response["type"] = responseType
		if responseType == "read_ok" {
			response["messages"] = mapToSlice()
		}
		node.Reply(msg, response)
	}

	go func() {
		for {
			time.Sleep(time.Millisecond * 500)
			syncReq := make(map[string]any)
			syncReq["type"] = "sync"
			syncReq["db"] = mapToSlice()
			for _, id := range nodeTopology {
				node.Send(id, syncReq)
			}
		}
	}()

	node.Handle("sync", func(msg maelstrom.Message) error {
		go func(msg maelstrom.Message) {
			var request SyncMessage
			if err := json.Unmarshal(msg.Body, &request); err != nil {
				log.Fatal("reading sync went wrong")
			}
			dbMutex.Lock()
			for _, v := range request.Db {
				db[v] = true
			}
			dbMutex.Unlock()
		}(msg)
		return nil
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		go func(msg maelstrom.Message) {
			var request BroadcastMessage
			if err := json.Unmarshal(msg.Body, &request); err != nil {
				log.Fatal("reading broadcast went wrong")
			}
			dbMutex.Lock()
			db[request.Message] = true
			dbMutex.Unlock()
			replyOk("broadcast_ok", msg)
		}(msg)
		return nil
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var request TopologyMessage
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			log.Fatal("reading topology went wrong")
		}
		nodeTopology = append(nodeTopology, request.Topology[msg.Dest]...)
		replyOk("topology_ok", msg)
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		go func() {
			replyOk("read_ok", msg)
		}()
		return nil
	})

	node.Run()
}
