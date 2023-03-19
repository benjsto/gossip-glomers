package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type AddMessageBody struct {
	MessageBody
	Delta int `json:"delta,omitempty"`
}

type ReadMessageBody struct {
	MessageBody
	Value int `json:"value"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body AddMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			breakLoop := false

			readVal, err := kv.ReadInt(context.Background(), "value")

			if err != nil {
				switch maelstrom.ErrorCode(err) {
				case maelstrom.KeyDoesNotExist:
					break
				default:
					return err
				}
			}

			newVal := readVal + body.Delta

			err = kv.CompareAndSwap(context.Background(), "value", readVal, newVal, true)

			if err == nil {
				break
			}

			switch maelstrom.ErrorCode(err) {
			case maelstrom.PreconditionFailed:
				time.Sleep(time.Millisecond * 200)
				continue
			default:
				breakLoop = true
			}

			if breakLoop {
				break
			}
		}

		body.Delta = 0
		body.Type = "add_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body ReadMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		readVal, err := kv.ReadInt(context.Background(), "value")

		if err != nil {
			switch maelstrom.ErrorCode(err) {
			case maelstrom.KeyDoesNotExist:
				break
			default:
				return fmt.Errorf("error reading from kv: %v", err)
			}
		}

		body.Value = readVal
		body.Type = "read_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
