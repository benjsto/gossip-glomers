package main

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type EchoMessageBody struct {
	MessageBody
	Echo any `json:"echo"`
}

type TopologyMessageBody struct {
	MessageBody
	Topology map[string][]string `json:"topology,omitempty"`
}

type GenerateMessageBody struct {
	MessageBody
	ID string `json:"id"`
}

type BroadcastMessageBody struct {
	MessageBody
	Message int `json:"message"`
}

type ReadMessageBody struct {
	MessageBody
	Messages []int `json:"messages"`
}

type BroadcastOkMessageBody struct {
	MessageBody
}

func main() {
	n := maelstrom.NewNode()

	neighbors := map[string]any{}

	messages := map[int]any{}
	messagesMu := sync.Mutex{}

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body EchoMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body.Type = "echo_ok"

		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body GenerateMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := uuid.New()

		body.Type = "generate_ok"
		body.ID = id.String()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, n := range body.Topology[n.ID()] {
			neighbors[n] = struct{}{}
		}

		body.Topology = map[string][]string{}
		body.Type = "topology_ok"

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgCopy := body.Message

		messagesMu.Lock()
		_, seen := messages[body.Message]
		messages[body.Message] = struct{}{}
		messagesMu.Unlock()

		err := n.Reply(msg, MessageBody{
			Type: "broadcast_ok",
		})

		rebroadcastMsg := BroadcastMessageBody{
			MessageBody: MessageBody{
				Type: "broadcast",
			},
			Message: msgCopy,
		}

		if !seen {
			neighborsToGossip := map[string]any{}
			mu := sync.Mutex{}

			for n := range neighbors {
				if n != msg.Src {
					neighborsToGossip[n] = neighbors[n]
				}
			}

			for {
				for node := range neighborsToGossip {
					n.RPC(node, rebroadcastMsg, func(m maelstrom.Message) error {
						var body MessageBody
						if err := json.Unmarshal(m.Body, &body); err != nil {
							return err
						}

						if body.Type == "broadcast_ok" {
							mu.Lock()
							delete(neighborsToGossip, node)
							mu.Unlock()
						} else {
							return errors.New("invalid response to broadcast message")
						}

						return nil
					})
				}

				time.Sleep(time.Millisecond * 5000)

				if len(neighborsToGossip) == 0 {
					break
				}
			}
		}

		return err
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body ReadMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body.Type = "read_ok"

		messagesMu.Lock()
		for msg := range messages {
			body.Messages = append(body.Messages, msg)
		}
		messagesMu.Unlock()

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
