package consumer

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub/v2"
	"github.com/HomayoonAlimohammadi/pubsub/gen/statepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Handler func(ctx context.Context, s *statepb.State, attrs map[string]string) error

type Consumer struct {
	client *pubsub.Client
	sub    *pubsub.Subscriber
}

func New(ctx context.Context, projectID, subID string) (*Consumer, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create pubsub client: %w", err)
	}

	return &Consumer{
		client: client,
		sub:    client.Subscriber(subID),
	}, nil
}

func (c *Consumer) Run(ctx context.Context, h Handler) error {
	return c.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		s := &statepb.State{}
		var err error

		switch msg.Attributes["googclient_schemaencoding"] {
		case "BINARY":
			err = proto.Unmarshal(msg.Data, s)
		case "JSON":
			err = protojson.Unmarshal(msg.Data, s)
		default:
			fmt.Println("invalid schema encoding??:", msg.Attributes["googclient_schemaencoding"])
			err = proto.Unmarshal(msg.Data, s)
		}
		if err != nil {
			log.Printf("decode failed (revision=%s): %v", msg.Attributes["googclient_schemarevisionid"], err)
			msg.Nack()
			return
		}

		if err := h(ctx, s, msg.Attributes); err != nil {
			msg.Nack()
			return
		}

		msg.Ack()
	})
}

func (c *Consumer) Close() {
	if c == nil || c.client == nil {
		return
	}
	c.client.Close()
}
