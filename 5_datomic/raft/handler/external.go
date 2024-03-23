package handler

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/savemaker/raft/service"
)

type ExternalHandler struct {
	node    *maelstrom.Node
	kvStore service.KeyValueStoreService
}

type ErrorResponse struct {
	Type string `json:"type"`
	Code int    `json:"code"`
	Text string `json:"text"`
}

func NewExternalHandler(
	node *maelstrom.Node,
	kvStore service.KeyValueStoreService,
) ExternalHandler {
	return ExternalHandler{
		node:    node,
		kvStore: kvStore,
	}
}

func (handler *ExternalHandler) Read(message *maelstrom.Message) {
	var request service.ReadRequestBody
	json.Unmarshal(message.Body, &request)

	response, err := handler.kvStore.Read(request)

	if err != nil {
		handler.handleErrors(err, message)
	} else {
		handler.node.Reply(*message, &response)
	}
}

func (handler *ExternalHandler) Write(message *maelstrom.Message) {
	var request service.WriteRequestBody
	json.Unmarshal(message.Body, &request)

	response, err := handler.kvStore.Write(request)

	if err != nil {
		handler.handleErrors(err, message)
	} else {
		handler.node.Reply(*message, &response)
	}
}

func (handler *ExternalHandler) CaS(message *maelstrom.Message) {
	var request service.CaSRequestBody
	json.Unmarshal(message.Body, &request)

	response, err := handler.kvStore.CaS(request)

	if err != nil {
		handler.handleErrors(err, message)
	} else {
		handler.node.Reply(*message, &response)
	}
}

func (handler *ExternalHandler) handleErrors(err error, message *maelstrom.Message) {
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
