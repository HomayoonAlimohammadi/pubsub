package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/HomayoonAlimohammadi/pubsub/gen/statepb"
	"github.com/HomayoonAlimohammadi/pubsub/internal/admin"
	"github.com/HomayoonAlimohammadi/pubsub/internal/consumer"
	"github.com/HomayoonAlimohammadi/pubsub/internal/publisher"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing subcommand: setup | publish | consume | commit | teardown")
	}

	switch args[0] {
	case "setup":
		return runSetup(ctx, args[1:])
	case "publish":
		return runPublish(ctx, args[1:])
	case "consume":
		return runConsume(ctx, args[1:])
	case "commit":
		return runCommit(ctx, args[1:])
	case "teardown":
		return runTeardown(ctx, args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func runSetup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("setup", flag.ContinueOnError)
	project := fs.String("project", "", "GCP project ID")
	schemaID := fs.String("schema", "", "schema ID")
	topicID := fs.String("topic", "", "topic ID")
	subID := fs.String("sub", "", "subscription ID")
	protoPath := fs.String("proto", "proto/state.proto", "path to proto file")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse setup flags: %w", err)
	}

	projectID, err := resolveProjectID(*project)
	if err != nil {
		return err
	}

	if _, err := admin.CreateSchema(ctx, projectID, *schemaID, *protoPath); err != nil {
		if admin.IsAlreadyExists(err) {
			log.Printf("schema %q already exists", *schemaID)
		} else {
			return err
		}
	}

	if err := admin.CreateTopicWithSchema(ctx, projectID, *topicID, *schemaID); err != nil {
		if admin.IsAlreadyExists(err) {
			log.Printf("topic %q already exists", *topicID)
		} else {
			return err
		}
	}

	if err := admin.CreateSubscription(ctx, projectID, *topicID, *subID); err != nil {
		if admin.IsAlreadyExists(err) {
			log.Printf("subscription %q already exists", *subID)
		} else {
			return err
		}
	}

	log.Printf("setup complete for schema=%s topic=%s sub=%s", *schemaID, *topicID, *subID)
	return nil
}

func runPublish(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	project := fs.String("project", "", "GCP project ID")
	topicID := fs.String("topic", "", "topic ID")
	name := fs.String("name", "", "state name")
	abbr := fs.String("abbr", "", "state abbreviation")
	population := fs.Int64("population", 0, "state population for schema v2")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse publish flags: %w", err)
	}

	*name = "test-name"
	*abbr = "test-abbr"
	*population = 100

	projectID, err := resolveProjectID(*project)
	if err != nil {
		return err
	}

	pub, err := publisher.New(ctx, projectID, *topicID)
	if err != nil {
		return err
	}
	defer pub.Close()

	msg := &statepb.State{
		Name:       *name,
		PostAbbr:   *abbr,
		Population: *population,
	}
	// if *population > 0 {
	// 	log.Printf("ignoring --population until proto/state.proto is upgraded to schema v2")
	// }

	msgID, err := pub.Publish(ctx, msg)
	if err != nil {
		return err
	}

	log.Printf("published message id=%s", msgID)
	return nil
}

func runConsume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	project := fs.String("project", "", "GCP project ID")
	subID := fs.String("sub", "", "subscription ID")
	durationValue := fs.String("duration", "30s", "receive duration")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse consume flags: %w", err)
	}

	projectID, err := resolveProjectID(*project)
	if err != nil {
		return err
	}

	duration, err := parseDurationFlag(*durationValue)
	if err != nil {
		return err
	}

	consumerClient, err := consumer.New(ctx, projectID, *subID)
	if err != nil {
		return err
	}
	defer consumerClient.Close()

	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	err = consumerClient.Run(runCtx, func(ctx context.Context, s *statepb.State, attrs map[string]string) error {
		if s == nil {
			return errors.New("decoded state is nil")
		}
		log.Printf("received state name=%s abbr=%s revision=%s population=%d", s.GetName(), s.GetPostAbbr(), attrs["googclient_schemarevisionid"], s.GetPopulation())
		fmt.Println("attrs:", attrs)
		return nil
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("consume messages: %w", err)
	}

	return nil
}

func runCommit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("commit", flag.ContinueOnError)
	project := fs.String("project", "", "GCP project ID")
	schemaID := fs.String("schema", "", "schema ID")
	protoPath := fs.String("proto", "proto/state.proto", "path to proto file")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse commit flags: %w", err)
	}

	projectID, err := resolveProjectID(*project)
	if err != nil {
		return err
	}

	revisionID, err := admin.CommitRevision(ctx, projectID, *schemaID, *protoPath)
	if err != nil {
		return err
	}

	log.Printf("committed schema revision=%s", revisionID)
	return nil
}

func runTeardown(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("teardown", flag.ContinueOnError)
	project := fs.String("project", "", "GCP project ID")
	schemaID := fs.String("schema", "", "schema ID")
	topicID := fs.String("topic", "", "topic ID")
	subID := fs.String("sub", "", "subscription ID")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse teardown flags: %w", err)
	}

	projectID, err := resolveProjectID(*project)
	if err != nil {
		return err
	}

	if err := admin.Teardown(ctx, projectID, *schemaID, *topicID, *subID); err != nil {
		return err
	}

	log.Printf("teardown complete for schema=%s topic=%s sub=%s", *schemaID, *topicID, *subID)
	return nil
}

func resolveProjectID(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}

	// projectID := os.Getenv("GCP_PROJECT_ID")
	// if projectID == "" {
	// 	return "", fmt.Errorf("resolve project ID: GCP_PROJECT_ID is not set and --project was not provided")
	// }

	return "operating-bird-497006-s8", nil
}

func parseDurationFlag(value string) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: %w", value, err)
	}

	return duration, nil
}
