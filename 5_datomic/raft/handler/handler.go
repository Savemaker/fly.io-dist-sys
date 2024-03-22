package handler

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/savemaker/raft/service"
)

type Handler interface {
	Read(message *maelstrom.Message)
	Write(message *maelstrom.Message)
	CaS(message *maelstrom.Message)
}

type RaftHandler struct {
	node    *maelstrom.Node
	service service.KeyValueStoreService
}

type ErrorResponse struct {
	Type string `json:"type"`
	Code int    `json:"code"`
	Text string `json:"text"`
}

func NewRaftHandler(node *maelstrom.Node) RaftHandler {
	return RaftHandler{
		node:    node,
		service: service.NewService(),
	}
}

func (handler *RaftHandler) Read(message *maelstrom.Message) {
	var request service.ReadRequestBody
	json.Unmarshal(message.Body, &request)

	response, err := handler.service.Read(request)

	if err != nil {
		handler.handleErrors(err, message)
	} else {
		handler.node.Reply(*message, &response)
	}
}

func (handler *RaftHandler) Write(message *maelstrom.Message) {
	var request service.WriteRequestBody
	json.Unmarshal(message.Body, &request)

	response, err := handler.service.Write(request)

	if err != nil {
		handler.handleErrors(err, message)
	} else {
		handler.node.Reply(*message, &response)
	}
}

func (handler *RaftHandler) CaS(message *maelstrom.Message) {
	var request service.CaSRequestBody
	json.Unmarshal(message.Body, &request)

	response, err := handler.service.CaS(request)

	if err != nil {
		handler.handleErrors(err, message)
	} else {
		handler.node.Reply(*message, &response)
	}
}

func (handler *RaftHandler) handleErrors(err error, message *maelstrom.Message) {
	if err != nil {
		errResponse := new(ErrorResponse)

		errResponse.Type = "error"
		errResponse.Text = err.Error()

		switch err := err.Error(); err {
		case service.ERR_NOT_FOUND:
			errResponse.Code = 20
		case service.ERR_CAS:
			errResponse.Code = 22
		}

		handler.node.Reply(*message, errResponse)
	}
}
