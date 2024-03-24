package handler

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	leaderelection "github.com/savemaker/raft/service/leader_election"
)

type InternalHandler struct {
	node                  *maelstrom.Node
	leaderElectionService *leaderelection.LeaderElectionService
}

func NewInternalHandler(
	node *maelstrom.Node,
	leaderElectionService *leaderelection.LeaderElectionService,
) InternalHandler {
	return InternalHandler{
		node:                  node,
		leaderElectionService: leaderElectionService,
	}
}

func (handler *InternalHandler) RequestVotes(message *maelstrom.Message) {
	var request map[string]any
	json.Unmarshal(message.Body, &request)

	src := message.Src
	term := leaderelection.Term(request["term"].(float64))
	lastLogIndex := int(request["lastLogIndex"].(float64))
	lastLogTerm := leaderelection.Term(request["lastLogTerm"].(float64))

	params := leaderelection.VoteGrantedRequest{
		CandidateID:           src,
		CandidateTerm:         term,
		CandidateLastLogTerm:  lastLogTerm,
		CandidateLastLogIndex: lastLogIndex,
	}

	response := handler.leaderElectionService.VoteGranted(params)

	handler.node.Reply(*message, map[string]any{
		"type":        "request_votes_ok",
		"term":        response.Term,
		"voteGranted": response.VoteGranted,
	})
}
