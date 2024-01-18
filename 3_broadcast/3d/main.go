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

	mapToSlice := func(db *map[int]bool) []int {
		dbMutex.Lock()
		defer dbMutex.Unlock()
		len := len(*db)
		slice := make([]int, len)
		i := 0
		for k := range *db {
			slice[i] = k
			i = i + 1
		}
		return slice
	}

	replyOk := func(responseType string, msg maelstrom.Message, db *map[int]bool) {
		response := make(map[string]any)

		response["type"] = responseType
		if responseType == "read_ok" {
			response["messages"] = mapToSlice(db)
		}
		node.Reply(msg, response)
	}

	go func(db *map[int]bool) {
		for {
			time.Sleep(time.Millisecond * 52)
			message := SyncMessage{Type: "sync", Db: mapToSlice(db)}
			for _, nid := range nodeTopology {
				if nid != node.ID() {
					go func(nid string, message SyncMessage) {
						node.Send(nid, message)
					}(nid, message)
				}
			}
		}
	}(&db)

	node.Handle("sync", func(msg maelstrom.Message) error {
		go func(msg maelstrom.Message, db *map[int]bool) {
			var request SyncMessage
			if err := json.Unmarshal(msg.Body, &request); err != nil {
				log.Fatal("reading sync went wrong")
			}
			dbMutex.Lock()
			ref := *db
			for _, v := range request.Db {
				ref[v] = true
			}
			dbMutex.Unlock()
		}(msg, &db)
		return nil
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		time.Sleep(500 * time.Millisecond)
		go func(msg maelstrom.Message, db *map[int]bool) {
			var request BroadcastMessage
			if err := json.Unmarshal(msg.Body, &request); err != nil {
				log.Fatal("reading broadcast went wrong")
			}
			dbMutex.Lock()
			ref := *db
			ref[request.Message] = true
			dbMutex.Unlock()

			replyOk("broadcast_ok", msg, nil)
		}(msg, &db)
		return nil
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var request TopologyMessage
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			log.Fatal("reading topology went wrong")
		}
		nodeTopology = append(nodeTopology, request.Topology[msg.Dest]...)
		replyOk("topology_ok", msg, nil)
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		time.Sleep(500 * time.Millisecond)
		go func(db *map[int]bool) {
			replyOk("read_ok", msg, db)
		}(&db)
		return nil
	})

	node.Run()
}
