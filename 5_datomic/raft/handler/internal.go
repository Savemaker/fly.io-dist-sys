package handler

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	leaderelection "github.com/savemaker/raft/service/leader_election"
)

type InternalHandler struct {
	stateService *leaderelection.StateService
}

func NewInternalHandler(stateService *leaderelection.StateService) InternalHandler {
	return InternalHandler{stateService: stateService}
}

func (handler *InternalHandler) RequestVotes(message *maelstrom.Message) {

}
