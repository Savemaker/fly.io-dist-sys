package handler

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/savemaker/raft/service"
)

type InternalHandler struct {
	stateService service.NodeState
}

func NewInternalHandler(stateService service.NodeState) InternalHandler {
	return InternalHandler{stateService: stateService}
}

func (handler *InternalHandler) RequestVotes(message *maelstrom.Message) {

}
