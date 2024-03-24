package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/savemaker/raft/handler"
	"github.com/savemaker/raft/service"
	leaderelection "github.com/savemaker/raft/service/leader_election"
)

func main() {
	node := maelstrom.NewNode()

	kvStore := service.NewKVStoreService()

	logService := leaderelection.NewLogService()
	stateService := leaderelection.NewStateService()
	termService := leaderelection.NewTermService()
	voteService := leaderelection.NewVoteService()

	leaderElectionService := leaderelection.NewLeaderElectionSerivce(
		node,
		logService,
		stateService,
		termService,
		voteService,
	)

	externalHandler := handler.NewExternalHandler(node, kvStore)

	internalHandler := handler.NewInternalHandler(node, leaderElectionService)

	node.Handle("read", func(msg maelstrom.Message) error {
		go externalHandler.Read(&msg)
		return nil
	})

	node.Handle("write", func(msg maelstrom.Message) error {
		go externalHandler.Write(&msg)
		return nil
	})

	node.Handle("cas", func(msg maelstrom.Message) error {
		go externalHandler.CaS(&msg)
		return nil
	})

	node.Handle("request_votes", func(msg maelstrom.Message) error {
		go internalHandler.RequestVotes(&msg)
		return nil
	})

	node.Run()
}
