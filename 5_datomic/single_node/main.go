package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TxnRequest struct {
	Type  string  `json:"type"`
	Txn   [][]any `json:"txn"`
	MsgId int     `json:"msg_id"`
}

func main() {
	node := maelstrom.NewNode()
	state := make(map[float64][]float64)

	node.Handle("txn", func(msg maelstrom.Message) error {
		req := unmarshalRequest(msg.Body)
		txResp := make([][]any, 0)

		for _, t := range req.Txn {
			op := t[0].(string)
			id := t[1].(float64)
			res := make([]any, 0)
			switch op {
			case "r":
				res = append(res, "r")
				res = append(res, id)
				val, ok := state[id]
				if ok {
					res = append(res, val)
				} else {
					res = append(res, nil)
				}
			case "append":
				toAppend := t[2].(float64)
				curSt, ok := state[id]
				if ok {
					state[id] = append(curSt, toAppend)
				} else {
					state[id] = append(make([]float64, 0), toAppend)
				}
				res = append(res, "append")
				res = append(res, id)
				res = append(res, toAppend)
			}
			txResp = append(txResp, res)
		}
		return node.Reply(msg, map[string]any{
			"type":        "txn_ok",
			"txn":         txResp,
			"in_reply_to": req.MsgId,
		})
	})

	node.Run()
}

func unmarshalRequest(body json.RawMessage) TxnRequest {
	var request TxnRequest
	json.Unmarshal(body, &request)
	return request
}
