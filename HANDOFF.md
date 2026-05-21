# Pub/Sub Proto Schema Demo — Handoff Doc

## Goal

Build a minimal Go CLI that exercises **Google Cloud Pub/Sub schemas with Protocol Buffers**, end-to-end:

1. Creates a proto-typed schema and a topic + subscription bound to it.
2. Publishes typed messages (validated server-side).
3. Consumes and decodes them.
4. Commits a new **schema revision** (adds an optional field) and verifies both old and new messages flow through.

Target: a developer running this against a real GCP project (no emulator). Authentication via Application Default Credentials.

---

## Prerequisites

- Go **1.22+**
- `protoc` installed locally + `protoc-gen-go` plugin:
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  ```
- `gcloud` CLI authenticated: `gcloud auth application-default login`
- A GCP project with Pub/Sub API enabled, env var `GCP_PROJECT_ID` set
- The acting principal needs roles: `roles/pubsub.editor` (for schema/topic admin) and `roles/pubsub.publisher` + `roles/pubsub.subscriber` (for runtime)

---

## Project layout to create

```
pubsub-schema-demo/
├── go.mod
├── proto/
│   └── state.proto            # source of truth for the schema
├── gen/
│   └── statepb/               # protoc output (generated)
├── internal/
│   ├── admin/admin.go         # schema + topic + subscription lifecycle
│   ├── publisher/publisher.go # one Publisher struct
│   └── consumer/consumer.go   # one Consumer struct
├── cmd/
│   └── demo/main.go           # CLI with subcommands
└── Makefile                   # convenience targets
```

Module path: `github.com/example/pubsub-schema-demo` (or whatever the user has set up).

---

## Dependencies

```
cloud.google.com/go/pubsub/v2          // v2 client — do NOT use v1
google.golang.org/protobuf
google.golang.org/grpc                 // transitive but list explicitly
```

Add via `go get`. The v2 import paths are:
- `cloud.google.com/go/pubsub/v2`
- `cloud.google.com/go/pubsub/v2/apiv1`
- `cloud.google.com/go/pubsub/v2/apiv1/pubsubpb`

---

## Task 1 — Define the proto

`proto/state.proto`:

```proto
syntax = "proto3";
package mypkg;
option go_package = "github.com/example/pubsub-schema-demo/gen/statepb;statepb";

message State {
  string name = 1;
  string post_abbr = 2;
}
```

**Critical constraint:** Pub/Sub stores the `.proto` text as the schema definition. The file must be self-contained — no `import` statements. One top-level message only.

---

## Task 2 — Codegen

Add a Makefile target:

```makefile
proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		proto/state.proto
	mkdir -p gen/statepb
	mv proto/state.pb.go gen/statepb/
```

Run `make proto` to produce `gen/statepb/state.pb.go`.

---

## Task 3 — Admin module

`internal/admin/admin.go` should expose:

```go
func CreateSchema(ctx context.Context, projectID, schemaID, protoPath string) (revisionID string, err error)
func CommitRevision(ctx context.Context, projectID, schemaID, protoPath string) (revisionID string, err error)
func CreateTopicWithSchema(ctx context.Context, projectID, topicID, schemaID string) error
func CreateSubscription(ctx context.Context, projectID, topicID, subID string) error
func Teardown(ctx context.Context, projectID, schemaID, topicID, subID string) error // best-effort cleanup
```

**Key implementation details:**

- Use `pubsub.NewClient(ctx, projectID)` from `cloud.google.com/go/pubsub/v2`. Schema, topic, and subscription admin are on `client.SchemaClient`, `client.TopicAdminClient`, `client.SubscriptionAdminClient`.
- For `CreateSchema`: read the `.proto` file contents and pass as `Definition`. Type is `pubsubpb.Schema_PROTOCOL_BUFFER`.
- For `CreateTopicWithSchema`: set `SchemaSettings`:
  ```go
  SchemaSettings: &pubsubpb.SchemaSettings{
      Schema:   fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
      Encoding: pubsubpb.Encoding_BINARY, // hard-code BINARY for this demo
      // Leave FirstRevisionId/LastRevisionId empty → all revisions accepted.
  }
  ```
- For `CreateSubscription`: bind to the topic. Configure a sane `AckDeadlineSeconds: 30`. **No DLQ needed for this demo**, but add a comment indicating where one would go.
- All ID args are short IDs (e.g., `my-topic`), not full resource paths. Build full paths inside the functions.
- `CommitRevision` calls `client.SchemaClient.CommitSchema` with the existing schema name and new definition. Return the new `RevisionId`.

---

## Task 4 — Publisher

`internal/publisher/publisher.go`:

```go
type Publisher struct {
    client    *pubsub.Client
    pub       *pubsub.Publisher
    encoding  pubsubpb.Encoding
}

func New(ctx context.Context, projectID, topicID string) (*Publisher, error)
func (p *Publisher) Publish(ctx context.Context, s *statepb.State) (msgID string, err error)
func (p *Publisher) Close()
```

**Implementation:**

- In `New`: call `GetTopic` **once** to read `SchemaSettings.Encoding`. Cache it. Error if `SchemaSettings == nil`.
- In `Publish`: switch on encoding:
  - `Encoding_BINARY`: `proto.Marshal(s)`
  - `Encoding_JSON`: `protojson.Marshal(s)`
- Call `p.pub.Publish(ctx, &pubsub.Message{Data: data})`, then `result.Get(ctx)` to surface validation errors.
- On `INVALID_ARGUMENT` from Pub/Sub: that means schema validation failed. Don't retry — log and return.
- `Close` calls `p.pub.Stop()` (flushes batches) then `p.client.Close()`.

---

## Task 5 — Consumer

`internal/consumer/consumer.go`:

```go
type Handler func(ctx context.Context, s *statepb.State, attrs map[string]string) error

type Consumer struct {
    client *pubsub.Client
    sub    *pubsub.Subscriber
}

func New(ctx context.Context, projectID, subID string) (*Consumer, error)
func (c *Consumer) Run(ctx context.Context, h Handler) error  // blocks until ctx cancelled
func (c *Consumer) Close()
```

**Implementation in `Run`:**

```go
return c.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
    s := &statepb.State{}
    var err error
    switch msg.Attributes["googclient_schemaencoding"] {
    case "BINARY":
        err = proto.Unmarshal(msg.Data, s)
    case "JSON":
        err = protojson.Unmarshal(msg.Data, s)
    default:
        err = proto.Unmarshal(msg.Data, s) // assume binary
    }
    if err != nil {
        log.Printf("decode failed (revision=%s): %v",
            msg.Attributes["googclient_schemarevisionid"], err)
        msg.Nack()
        return
    }
    if err := h(ctx, s, msg.Attributes); err != nil {
        msg.Nack()
        return
    }
    msg.Ack()
})
```

The handler `h` receives the decoded message and the attributes (so it can log the revision ID for verification).

---

## Task 6 — CLI

`cmd/demo/main.go` should support these subcommands (use `flag` package, no Cobra needed):

| Command | Action |
|---|---|
| `setup` | Creates schema (v1), topic, subscription |
| `publish` | Publishes one `State{name, post_abbr}` message; prints msg ID + matched revision (re-fetch via `GetTopic` is not needed; we'll log the revision attribute on consume) |
| `consume` | Runs the consumer for N seconds (e.g. 30s), logs each decoded message + `googclient_schemarevisionid` |
| `commit-v2` | Commits revision 2 (the updated proto, see Task 8); prints the new revision ID |
| `teardown` | Deletes sub, topic, and schema |

Flags: `--project`, `--schema`, `--topic`, `--sub`, `--proto` (path to `.proto` file), `--name`, `--abbr`, `--population` (for v2 publishes), `--duration` (consumer run length).

Pull `GCP_PROJECT_ID` from env if `--project` is omitted.

---

## Task 7 — Smoke test (v1)

```bash
export GCP_PROJECT_ID=your-project

make proto
go build -o bin/demo ./cmd/demo

bin/demo setup --schema=demo-schema --topic=demo-topic --sub=demo-sub --proto=proto/state.proto
bin/demo publish --topic=demo-topic --name=Alaska --abbr=AK
bin/demo publish --topic=demo-topic --name=Texas --abbr=TX
bin/demo consume --sub=demo-sub --duration=15s
```

Expected: both messages decode, logs show `revision=<8-char-id>` matching the first revision.

---

## Task 8 — Versioning exercise (the main event)

### 8a. Edit the proto

Modify `proto/state.proto` to add ONE optional field:

```proto
syntax = "proto3";
package mypkg;
option go_package = "github.com/example/pubsub-schema-demo/gen/statepb;statepb";

message State {
  string name = 1;
  string post_abbr = 2;
  optional int64 population = 3;  // NEW — MUST be `optional` to satisfy Pub/Sub rules
}
```

**Why `optional`:** Pub/Sub's proto schema compatibility rule is: *you can add or remove optional fields; you cannot add or delete other fields; you cannot edit existing fields.* In proto3, only fields declared with the `optional` keyword count as optional for this purpose. Without it, you can never remove the field via a future revision.

Re-run `make proto` to regenerate.

### 8b. Commit revision 2

```bash
bin/demo commit-v2 --schema=demo-schema --proto=proto/state.proto
# Prints new 8-char revision ID
```

Behind the scenes this calls `SchemaClient.CommitSchema` with the same schema name and the updated `.proto` contents.

No topic update is required — the topic was created with an open revision range, so it accepts any committed revision automatically.

### 8c. Publish with the new field, consume, observe

```bash
bin/demo publish --topic=demo-topic --name=California --abbr=CA --population=39000000
bin/demo consume --sub=demo-sub --duration=15s
```

Expected: the message decodes, logged revision matches revision 2. Pub/Sub validates newest-revision-first.

### 8d. Publish a message that omits the new field

```bash
bin/demo publish --topic=demo-topic --name=Nevada --abbr=NV
bin/demo consume --sub=demo-sub --duration=15s
```

Expected: decodes cleanly. The revision attribute may show either revision 1 or revision 2 — Pub/Sub matches against the newest revision the message satisfies, and since `population` is optional, this message satisfies both. Newest wins, so expect revision 2.

### 8e. (Negative test, optional) Try an incompatible change

To prove the rule, attempt to commit a revision that **renames** `post_abbr` to `abbreviation`:

```proto
message State {
  string name = 1;
  string abbreviation = 2;  // renamed from post_abbr
  optional int64 population = 3;
}
```

`bin/demo commit-v2 --schema=demo-schema --proto=proto/state.proto` should fail with an `INVALID_ARGUMENT` from `CommitSchema`. Confirm the error surfaces clearly.

---

## Pub/Sub-specific compatibility rules (cheat sheet)

| Change to `.proto` | `CommitSchema` accepts? |
|---|---|
| Add `optional` field with new tag number | ✅ |
| Remove `optional` field | ✅ |
| Add non-optional / `repeated` / nested message field | ❌ |
| Remove any non-optional field | ❌ |
| Rename a field | ❌ |
| Change a field's type | ❌ |
| Change a field's tag number | ❌ |

Plus: max 20 revisions per schema, schema definition ≤ 300 KB, schema name format `projects/{p}/schemas/{id}`.

---

## Implementation gotchas the agent must respect

1. **Use v2 client only.** `cloud.google.com/go/pubsub/v2`. v1 (`pubsub.NewClient(...).Topic(...)`) is deprecated and has a different API surface.
2. **Don't call `GetTopic` from the consumer.** Consumers must rely on the `googclient_schemaencoding` attribute on each message, not on topic admin metadata.
3. **Don't `GetTopic` per publish.** Fetch encoding once at `Publisher` construction and cache it.
4. **One top-level message in `state.proto`.** No `import` statements. No nested message types referenced from another file.
5. **The proto3 `optional` keyword matters.** Without it, a field is not "optional" from Pub/Sub's perspective, even though proto3 singular fields have default-value semantics. Mark every new field `optional` unless you're certain it's permanent.
6. **All admin operations are idempotent-by-error.** `CreateSchema` / `CreateTopic` / `CreateSubscription` return `ALREADY_EXISTS` if already there. The CLI's `setup` command should treat `ALREADY_EXISTS` as success (log and continue), not as failure.
7. **Auth:** rely on ADC. Do NOT accept a key-file path. Do NOT hard-code credentials anywhere.
8. **Reuse `*pubsub.Client`.** Don't open a new client per command call inside the same process.
9. **`pub.Stop()` is mandatory** before exiting the publisher, or in-flight batches may be lost.

---

## Acceptance checklist (what "done" looks like)

- [ ] `make proto` succeeds and produces `gen/statepb/state.pb.go`.
- [ ] `bin/demo setup` creates schema, topic, sub (or no-ops if already created).
- [ ] `bin/demo publish` succeeds for a valid `State` message; fails with a clear error for an invalid one.
- [ ] `bin/demo consume` decodes and prints messages, including the matched schema revision ID.
- [ ] `bin/demo commit-v2` with the proto from Task 8a returns a new revision ID.
- [ ] After commit-v2, publishing with `--population` and without both work; consumer logs show revision 2 stamped on both.
- [ ] `bin/demo commit-v2` with an incompatible proto fails fast with `INVALID_ARGUMENT`.
- [ ] `bin/demo teardown` removes all created resources cleanly.
- [ ] No `panic`s; all errors wrapped with `fmt.Errorf("...: %w", err)`.
- [ ] Code uses only the v2 Pub/Sub client; no v1 imports anywhere.

---

## Out of scope (do NOT implement)

- DLQ subscription
- Avro support
- JSON encoding (BINARY only)
- Schema rollback
- Revision pinning via `FirstRevisionId` / `LastRevisionId`
- Integration tests / mocks
- Concurrency tuning beyond defaults

These are all valid follow-ups but distract from the core exercise.
