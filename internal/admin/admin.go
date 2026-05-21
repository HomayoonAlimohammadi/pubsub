package admin

import (
	"context"
	"errors"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub/v2"
	pubsubapiv1 "cloud.google.com/go/pubsub/v2/apiv1"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func CreateSchema(ctx context.Context, projectID, schemaID, protoPath string) (string, error) {
	definition, err := os.ReadFile(protoPath)
	if err != nil {
		return "", fmt.Errorf("read proto schema %q: %w", protoPath, err)
	}

	schemaClient, err := pubsubapiv1.NewSchemaClient(ctx)
	if err != nil {
		return "", fmt.Errorf("create schema client: %w", err)
	}
	defer schemaClient.Close()

	resp, err := schemaClient.CreateSchema(ctx, &pubsubpb.CreateSchemaRequest{
		Parent:   fmt.Sprintf("projects/%s", projectID),
		SchemaId: schemaID,
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_PROTOCOL_BUFFER,
			Definition: string(definition),
		},
	})
	if err != nil {
		return "", fmt.Errorf("create schema %q: %w", schemaID, err)
	}

	return resp.GetRevisionId(), nil
}

func CommitRevision(ctx context.Context, projectID, schemaID, protoPath string) (string, error) {
	definition, err := os.ReadFile(protoPath)
	if err != nil {
		return "", fmt.Errorf("read proto schema %q: %w", protoPath, err)
	}

	schemaClient, err := pubsubapiv1.NewSchemaClient(ctx)
	if err != nil {
		return "", fmt.Errorf("create schema client: %w", err)
	}
	defer schemaClient.Close()

	resp, err := schemaClient.CommitSchema(ctx, &pubsubpb.CommitSchemaRequest{
		Name: fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_PROTOCOL_BUFFER,
			Definition: string(definition),
		},
	})
	if err != nil {
		return "", fmt.Errorf("commit schema revision %q: %w", schemaID, err)
	}

	return resp.GetRevisionId(), nil
}

func CreateTopicWithSchema(ctx context.Context, projectID, topicID, schemaID string) error {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("create pubsub client: %w", err)
	}
	defer client.Close()

	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
		SchemaSettings: &pubsubpb.SchemaSettings{
			Schema:   fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
			Encoding: pubsubpb.Encoding_BINARY,
		},
	})
	if err != nil {
		return fmt.Errorf("create topic %q: %w", topicID, err)
	}

	return nil
}

func CreateSubscription(ctx context.Context, projectID, topicID, subID string) error {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("create pubsub client: %w", err)
	}
	defer client.Close()

	_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID),
		Topic:              fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
		AckDeadlineSeconds: 30,
		// Dead letter policy would be configured here for a production workflow.
	})
	if err != nil {
		return fmt.Errorf("create subscription %q: %w", subID, err)
	}

	return nil
}

func Teardown(ctx context.Context, projectID, schemaID, topicID, subID string) error {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("create pubsub client: %w", err)
	}
	defer client.Close()

	var joined error

	if err := client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{
		Subscription: fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID),
	}); err != nil && !isNotFound(err) {
		joined = errors.Join(joined, fmt.Errorf("delete subscription %q: %w", subID, err))
	}

	if err := client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{
		Topic: fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
	}); err != nil && !isNotFound(err) {
		joined = errors.Join(joined, fmt.Errorf("delete topic %q: %w", topicID, err))
	}

	schemaClient, err := pubsubapiv1.NewSchemaClient(ctx)
	if err != nil {
		return errors.Join(joined, fmt.Errorf("create schema admin client: %w", err))
	}
	defer schemaClient.Close()

	if err := schemaClient.DeleteSchema(ctx, &pubsubpb.DeleteSchemaRequest{
		Name: fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
	}); err != nil && !isNotFound(err) {
		joined = errors.Join(joined, fmt.Errorf("delete schema %q: %w", schemaID, err))
	}

	return joined
}

func IsAlreadyExists(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}
