package publisher

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/HomayoonAlimohammadi/pubsub/gen/statepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Publisher struct {
	client   *pubsub.Client
	pub      *pubsub.Publisher
	encoding pubsubpb.Encoding
}

func New(ctx context.Context, projectID, topicID string) (*Publisher, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create pubsub client: %w", err)
	}

	topic, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
		Topic: fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
	})
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("get topic %q: %w", topicID, err)
	}
	if topic.GetSchemaSettings() == nil {
		client.Close()
		return nil, fmt.Errorf("get topic %q: missing schema settings", topicID)
	}

	return &Publisher{
		client:   client,
		pub:      client.Publisher(topicID),
		encoding: topic.GetSchemaSettings().GetEncoding(),
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, s *statepb.State) (string, error) {
	var (
		data []byte
		err  error
	)

	switch p.encoding {
	case pubsubpb.Encoding_JSON:
		data, err = protojson.Marshal(s)
	default:
		data, err = proto.Marshal(s)
	}
	if err != nil {
		return "", fmt.Errorf("marshal state message: %w", err)
	}

	result := p.pub.Publish(ctx, &pubsub.Message{Data: data})
	msgID, err := result.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			return "", fmt.Errorf("publish rejected by schema validation: %w", err)
		}
		return "", fmt.Errorf("publish state message: %w", err)
	}

	return msgID, nil
}

func (p *Publisher) Close() {
	if p == nil {
		return
	}
	if p.pub != nil {
		p.pub.Stop()
	}
	if p.client != nil {
		p.client.Close()
	}
}
