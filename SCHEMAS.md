# Pub/Sub Schema Validation with Protobuf

A reference for engineering teams adopting Google Cloud Pub/Sub schemas with **Protocol Buffers**. It covers the conceptual model, the rules that govern schema evolution, recommended client patterns with the Go v2 SDK, and the gotchas that bite teams in production.

It is deliberately not tied to any one codebase. Code snippets are illustrative — adapt them to your service layout.

---

## 1. Concept model

### 1.1 What a Pub/Sub schema is

A Pub/Sub schema is a **server-side resource** (`projects/{p}/schemas/{id}`) that stores a single proto file's text as the contract for a topic's payloads. It is independent of any topic — you create the schema first, then attach it to one or more topics through their `SchemaSettings`. After attachment, every `Publish` request to that topic is validated against the schema before Pub/Sub accepts it; non-conforming payloads are rejected with `INVALID_ARGUMENT` and never reach a subscription.

Two schema **types** are supported: `AVRO` and `PROTOCOL_BUFFER`. This document covers only the proto path.

A schema can live in a different GCP project from the topic that uses it, as long as the publisher's principal has `pubsub.schemas.attach` on the schema's project.

### 1.2 Revisions

Each schema is a stack of immutable **revisions**. The first revision is created by `CreateSchema`; subsequent revisions are added by `CommitSchema`. Each revision gets an auto-generated 8-character ID (e.g. `a1b2c3d4`) and is stamped on every message that validates against it (see [Attributes the server adds](#15-attributes-the-server-adds)).

Hard limits to remember:

| Limit | Value |
|---|---|
| Revisions per schema | 20 (delete an old one to add a new one beyond this) |
| Schema definition size | 300 KB |
| Schemas per project | 10,000 |

A topic doesn't bind to a single revision — it binds to a **range** (`first_revision_id`, `last_revision_id`). When the range is left open (both unset), the topic accepts the oldest existing revision through the newest one as new ones are committed. This is the common choice for additive evolution: commit a new revision and clients can immediately publish against it without any topic update.

### 1.3 Encoding (BINARY vs JSON)

A topic's `SchemaSettings.Encoding` declares what the *wire format* of `Message.Data` must be:

- `BINARY` — `proto.Marshal` output. Compact, no field-name overhead, the right default for service-to-service traffic.
- `JSON` — `protojson.Marshal` output. Human-readable, larger, useful when consumers are heterogeneous or when payloads need to be inspectable by humans.

Encoding is **fixed per topic**. Changing it on a live topic breaks active publishers with `INVALID_ARGUMENT`. Pick at topic-creation time and treat it as immutable.

Publishers and consumers discover the encoding differently:

- Publishers: fetch `topic.SchemaSettings.Encoding` **once at startup** and cache it. Never per-publish.
- Consumers: read the per-message attribute `googclient_schemaencoding`. Never via `GetTopic`.

### 1.4 Validation at publish time

When a publish lands at a schema-bound topic:

1. Pub/Sub iterates the topic's allowed revision range **newest-first**.
2. For each revision, it tries to parse `Message.Data` in the topic's encoding against that revision's proto definition.
3. The **first revision that parses successfully wins** — the message is accepted and stamped with that revision's ID.
4. If no revision matches, the publish fails synchronously with gRPC `INVALID_ARGUMENT`. The Go v2 SDK surfaces this from the `*PublishResult.Get(ctx)` call.

Two consequences of newest-first matching that teams routinely miss:

- **A message produced by a v1-aware client may stamp as v2.** If v2 added only optional fields, the v1 payload still parses against v2; the server stops at v2 (the first match in newest-first order) and stamps v2's revision ID. Don't use the revision attribute as a proxy for "which client version produced this." If you need that, encode the producer version inside the payload itself.
- **A v2-aware client can publish payloads that fail against v1.** That is the point of revisions: clients can adopt new fields safely as soon as v2 is committed, while older clients keep working unchanged.

### 1.5 Attributes the server adds

On every successfully validated publish, Pub/Sub stamps three reserved attributes onto the message:

| Attribute key | Value |
|---|---|
| `googclient_schemaname` | Full schema resource name (`projects/{p}/schemas/{id}`) |
| `googclient_schemarevisionid` | 8-char revision ID that matched |
| `googclient_schemaencoding` | `BINARY` or `JSON` |

These are reserved keys — applications must not write to them. Consumers should treat `googclient_schemaencoding` as the **only** source of truth about how to decode the payload, because the topic's current `SchemaSettings.Encoding` can be edited out from under in-flight messages and because consumers may legitimately receive messages stamped at past revisions.

Log `googclient_schemarevisionid` on every received message. It is the only durable handle you have for the question "which revision did this message validate against?" — necessary for diagnosing decode failures, audit trails, and corruption investigations.

---

## 2. Schema evolution: what you can and cannot change

Pub/Sub enforces a **compatibility check** on `CommitSchema`. For `PROTOCOL_BUFFER` schemas the documented rule is narrow: *you may add or remove `optional` fields, and nothing else*. Concretely:

| Change to `.proto` | Accepted? | Notes |
|---|---|---|
| Add a field declared `optional` with a new tag | yes | The intended path for additive evolution. |
| Remove a field that was declared `optional` | yes | Old payloads with the field still decode (the value becomes an unknown field, then is dropped on re-serialization). |
| Add a non-`optional` / `repeated` / nested-message field | rejected per docs | The compatibility rule treats only `optional` fields as additive. |
| Remove a non-`optional` field | no | |
| Rename a field (same tag, new name) | no | |
| Change a field's type | no | |
| Change a field's tag number | no | |

**The proto3 `optional` keyword is load-bearing.** Proto3 historically removed `optional`, then reintroduced it in 3.15 specifically to give singular fields *explicit presence tracking*. Pub/Sub's compatibility check leans on this distinction: a field declared `optional` is treated as truly optional and can be added or removed in future revisions; a singular field without `optional` is treated as part of the schema's permanent shape and cannot be removed.

*A note on proto3 wire-format reality:* on the wire, every proto3 singular field is forward/backward compatible — old readers ignore unknown fields, new readers see defaults for missing ones. Pub/Sub's check is therefore stricter than wire-level compatibility would require. The practical guidance: **always mark new fields `optional`**, even when wire-format intuition says you don't need to, because the server-side compatibility check is what gates your future flexibility, not wire format.

If a commit fails, the SDK returns gRPC `INVALID_ARGUMENT` from `SchemaClient.CommitSchema`. This is `codes.InvalidArgument`, distinct from `AlreadyExists` and `NotFound`. The error message usually identifies the offending change.

### 2.1 Constraints on the proto definition itself

- **No `import` statements.** The server stores the file text verbatim and parses it standalone — references to other proto files cannot be resolved.
- **Exactly one top-level message.** Nested messages declared inline are fine; multiple top-level messages are not.
- **Single file.** There is no notion of a multi-file schema.

These constraints are why teams that already have rich proto libraries usually carve out a small, dedicated `.proto` file per Pub/Sub schema rather than reusing existing service protos.

### 2.2 Other operations on the schema stack

- **`RollbackSchema(schemaID, revisionID)`** does *not* revert state — it **creates a new revision** whose definition is a byte-identical copy of an older one. Intervening revisions remain in the history, and messages stamped against them still resolve. Treat rollback as "publish a hotfix revision identical to the last known good," not as "rewind."
- **`DeleteSchemaRevision`** is allowed as long as the schema has more than one revision. If a topic's revision range includes the deleted ID, validation skips it; the effective `first`/`last` shift to surviving neighbors. Messages stamped with the deleted revision keep that ID in their attributes, but `GetSchema(...@revision)` for it returns `NOT_FOUND`. **BigQuery subscriptions with schema validation enabled will fail to write** messages whose validating revision has been deleted. Prefer rollback over delete-revision in production unless you are certain no downstream consumer needs to resolve the revision.
- **`DeleteSchema`** is destructive: all attempts to publish to topics bound to it begin failing immediately. You cannot delete a schema that is still bound to a topic — first either remove the binding via topic update, or delete the topic. Schema deletion is permanent; recreating the same `schemaID` afterwards gives you a completely independent schema with a fresh first revision.
- **`ValidateSchema(definition)`** checks that a proto file parses server-side. It does *not* check compatibility against prior revisions — that gate is only enforced inside `CommitSchema`.
- **`ValidateMessage`** takes either a schema name or an inline definition plus an encoding, and tells you whether a candidate payload would be accepted. Useful in unit tests that want to assert "this struct serializes to something the live schema accepts" without round-tripping through a real publish.

---

## 3. End-to-end lifecycle

```
state.proto  --(CreateSchema)-->  schema (revision r1)
                                        |
                                        v
                    (CreateTopic with SchemaSettings)
                                        |
                                        v
                                    topic ---(CreateSubscription)---> subscription
                                        |
                                        v
                        Publish(proto.Marshal(msg))
                                        |
                    server picks newest matching revision (rN)
                                        |
                                        v
                    stamps googclient_schemarevisionid=rN
                                        |
                                        v
                    Receive → switch on googclient_schemaencoding → proto.Unmarshal

later:
  edit state.proto (add optional field) → CommitSchema → revision r2
  new publishes carrying the new field validate against r2
  old publishes omitting the new field validate against r2 too (newest-first),
    and stamp r2 — even though they could have matched r1
```

There is no client-side schema fetch on the message path. The publisher learns the encoding once at startup. The consumer learns the encoding (and revision ID) from per-message attributes. The schema resource itself is consulted only by `CreateSchema`/`CommitSchema`/admin tooling.

---

## 4. Recommended client patterns (Go v2 SDK)

All snippets assume the v2 Pub/Sub client. **Do not use the v1 client** (`cloud.google.com/go/pubsub`) — it is deprecated and has a meaningfully different API surface for schemas. Import:

```go
import (
    "cloud.google.com/go/pubsub/v2"
    "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/encoding/protojson"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)
```

### 4.1 Admin: create schema, topic, subscription

```go
client, err := pubsub.NewClient(ctx, projectID)
if err != nil { return err }
defer client.Close()

// 1. Create schema (first revision)
definition, _ := os.ReadFile("state.proto")
schemaResp, err := client.SchemaClient.CreateSchema(ctx, &pubsubpb.CreateSchemaRequest{
    Parent:   "projects/" + projectID,
    SchemaId: schemaID,
    Schema: &pubsubpb.Schema{
        Type:       pubsubpb.Schema_PROTOCOL_BUFFER,
        Definition: string(definition),
    },
})
// schemaResp.RevisionId is the 8-char ID of the first revision.

// 2. Create topic with the schema attached.
//    Leaving first/last_revision_id empty makes the topic accept any future revision.
_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
    Name: fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
    SchemaSettings: &pubsubpb.SchemaSettings{
        Schema:   fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
        Encoding: pubsubpb.Encoding_BINARY,
    },
})

// 3. Create subscription. Schema is inherited from the topic; nothing extra to configure.
_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
    Name:               fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID),
    Topic:              fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
    AckDeadlineSeconds: 30,
})
```

Notes:

- `CreateSchema`/`CreateTopic`/`CreateSubscription` are **not idempotent server-side** — they return gRPC `AlreadyExists`. Treat that as success in setup tooling: `status.Code(err) == codes.AlreadyExists`.
- The same `*pubsub.Client` exposes `SchemaClient`, `TopicAdminClient`, and `SubscriptionAdminClient`. Reuse one client across admin operations rather than constructing a new one per call.
- Teardown order is sub → topic → schema. A schema bound to a topic cannot be deleted, and a topic with active subscriptions cannot be deleted cleanly. Tolerate `NotFound` on each step so reruns are safe.

### 4.2 Publisher: cache encoding, marshal, surface validation errors

```go
type Publisher struct {
    client   *pubsub.Client
    pub      *pubsub.Publisher
    encoding pubsubpb.Encoding
}

func NewPublisher(ctx context.Context, projectID, topicID string) (*Publisher, error) {
    client, err := pubsub.NewClient(ctx, projectID)
    if err != nil { return nil, err }

    // Fetch SchemaSettings.Encoding ONCE. Cache it for the life of the process.
    topic, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
        Topic: fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
    })
    if err != nil { client.Close(); return nil, err }
    if topic.GetSchemaSettings() == nil {
        client.Close()
        return nil, fmt.Errorf("topic %q has no schema attached", topicID)
    }

    return &Publisher{
        client:   client,
        pub:      client.Publisher(topicID),
        encoding: topic.GetSchemaSettings().GetEncoding(),
    }, nil
}

func (p *Publisher) Publish(ctx context.Context, m proto.Message) (string, error) {
    var data []byte
    var err error
    switch p.encoding {
    case pubsubpb.Encoding_JSON:
        data, err = protojson.Marshal(m)
    default: // BINARY
        data, err = proto.Marshal(m)
    }
    if err != nil { return "", fmt.Errorf("marshal: %w", err) }

    result := p.pub.Publish(ctx, &pubsub.Message{Data: data})
    msgID, err := result.Get(ctx)
    if err != nil {
        if status.Code(err) == codes.InvalidArgument {
            return "", fmt.Errorf("schema validation rejected publish: %w", err)
        }
        return "", err
    }
    return msgID, nil
}

func (p *Publisher) Close() {
    p.pub.Stop()    // flush in-flight batches FIRST
    p.client.Close()
}
```

Why these shapes:

- **One `GetTopic` at startup** instead of per publish — `GetTopic` is an admin RPC, an order of magnitude heavier than `Publish`, and the encoding never changes for the life of a process.
- **`pub.Stop()` before `client.Close()`** — the v2 `Publisher` batches under the hood. Closing the client without stopping the publisher drops in-flight batches silently. The ordering is mandatory.
- **`codes.InvalidArgument` distinguished from transient errors** — schema validation failures are deterministic and should not be retried; transient errors (`Unavailable`, `DeadlineExceeded`) should. Mixing them up wastes capacity and obscures real bugs.

### 4.3 Consumer: attribute-driven decode, log the revision

```go
type Handler func(ctx context.Context, m *mypb.State, attrs map[string]string) error

func Run(ctx context.Context, projectID, subID string, h Handler) error {
    client, err := pubsub.NewClient(ctx, projectID)
    if err != nil { return err }
    defer client.Close()

    return client.Subscriber(subID).Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
        m := &mypb.State{}

        switch msg.Attributes["googclient_schemaencoding"] {
        case "BINARY":
            if err := proto.Unmarshal(msg.Data, m); err != nil {
                log.Printf("decode failed (revision=%s): %v",
                    msg.Attributes["googclient_schemarevisionid"], err)
                msg.Nack()
                return
            }
        case "JSON":
            if err := protojson.Unmarshal(msg.Data, m); err != nil {
                log.Printf("decode failed (revision=%s): %v",
                    msg.Attributes["googclient_schemarevisionid"], err)
                msg.Nack()
                return
            }
        default:
            // Fail loudly. An unknown encoding means either the topic config
            // changed under us or a non-schema message is on this subscription.
            // Silently falling back to proto.Unmarshal will produce garbage.
            log.Printf("unknown encoding %q (revision=%s)",
                msg.Attributes["googclient_schemaencoding"],
                msg.Attributes["googclient_schemarevisionid"])
            msg.Nack()
            return
        }

        if err := h(ctx, m, msg.Attributes); err != nil {
            msg.Nack()
            return
        }
        msg.Ack()
    })
}
```

Why these shapes:

- **Encoding from the message, not from `GetTopic`** — the topic's current encoding may differ from the encoding under which an in-flight message was published, and consumers must remain correct against any historical revision.
- **No silent default branch** — if `googclient_schemaencoding` is unset or unrecognized, you cannot pick the right unmarshaler safely. Nack and surface the anomaly. A consumer that "always works" by guessing is a consumer that quietly corrupts data.
- **Log the revision ID** — every receive logs `googclient_schemarevisionid`. Without it, post-incident questions about which revision produced bad data are unanswerable.

### 4.4 Producing a new revision

```go
definition, _ := os.ReadFile("state.proto")
resp, err := client.SchemaClient.CommitSchema(ctx, &pubsubpb.CommitSchemaRequest{
    Name: fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
    Schema: &pubsubpb.Schema{
        Name:       fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
        Type:       pubsubpb.Schema_PROTOCOL_BUFFER,
        Definition: string(definition),
    },
})
if err != nil {
    if status.Code(err) == codes.InvalidArgument {
        // Compatibility check failed — see §2 for what's allowed.
    }
    return err
}
// resp.RevisionId is the new 8-char revision.
```

A topic with an open revision range starts validating new publishes against this revision **a few minutes later** (see the propagation-lag note in §5). If you have automated tests that commit then immediately publish exercising a new field, expect intermittent `INVALID_ARGUMENT` until propagation completes.

---

## 5. Considerations and gotchas

The patterns below are the failure modes a senior engineer should know before shipping anything that depends on Pub/Sub schemas.

**Use `optional` on every field you might ever remove.** Pub/Sub's compatibility check treats proto3 singular fields without `optional` as non-optional and rejects their removal in future revisions. A field committed without `optional` is permanent — you can never remove it via `CommitSchema`, only by deleting the entire schema (which breaks all publishers). The cost of `optional` is zero; the cost of forgetting it is permanent.

**Encoding is fixed; changing it on a live topic breaks publishers.** A topic's `SchemaSettings.Encoding` is treated as immutable in practice. Publishers cache it. Changing it requires a coordinated client redeploy with the topic update, which is rarely worth the cost. Pick at creation time and commit.

**Reuse a single `*pubsub.Client`.** Constructing a new client per operation is wasteful (each opens fresh gRPC connections) and undermines the SDK's batching. Long-lived services should hold one client for the process lifetime.

**`pub.Stop()` before `client.Close()`, every time.** The publisher batches in the background. Closing the client without stopping the publisher drops batched messages. This is the most common silent data-loss bug in Pub/Sub v2 code.

**Revision matching is newest-first.** A message that omits a new optional field still validates against the newest revision and is stamped with its ID. Don't infer client version from `googclient_schemarevisionid`; if you need producer identity, embed it in the payload.

**Open revision ranges adopt new revisions automatically.** This is what makes `CommitSchema` plus "publish the new field" work without touching the topic. The flip side: every revision becomes valid the moment it is committed; revisions are not opt-in per topic. If you need stricter control (e.g. canary the new revision against a subset of topics), pin `first_revision_id` and `last_revision_id` explicitly on each topic and update them as part of a coordinated rollout.

**Propagation lag is real.** Revision and revision-range changes take "a few minutes" — typically single-digit minutes — to propagate. Tests that commit-then-publish in tight loops will be flaky. Build retry-with-backoff into commit/publish integration tests, or accept an explicit wait.

**Schema deletion does not cascade gracefully.** Deleting a schema while topics still reference it makes those topics start failing publishes with `INVALID_ARGUMENT`. Always update topics to remove `SchemaSettings` (or delete the topics) before deleting the schema.

**Delete-revision is hazardous for BigQuery subscriptions.** If you use BigQuery subscriptions with schema validation, deleted revisions strand any in-flight or replayed messages stamped with that revision — they cannot be written. Prefer `RollbackSchema` over `DeleteSchemaRevision` for "undo" semantics.

**Consumers must not call `GetTopic` or `GetSchema` on the receive path.** Both add latency, both consume admin RPC budget, and both race against admin edits. The per-message attributes carry everything a consumer needs.

**Auth is ADC.** Don't accept a service-account key file. Rely on Application Default Credentials. In GKE/Cloud Run, use Workload Identity; on a dev laptop, `gcloud auth application-default login`. Required roles: `roles/pubsub.editor` for admin tools, `roles/pubsub.publisher` + `roles/pubsub.subscriber` for runtime services.

**Validation is opt-in per topic, not per project.** A schema can exist with zero topics bound to it — it's just metadata until attached. Conversely, a topic without `SchemaSettings` accepts arbitrary payloads even if a same-named schema exists. The binding is what makes validation happen.

---

## 6. Operational extras

Capabilities not strictly required for a minimum-viable setup, but standard in production tooling:

- **`ValidateSchema(definition)`** — pre-flight check that a `.proto` file parses server-side. Useful in CI as a guardrail before merging proto changes. Does not check backward compatibility — that gate is `CommitSchema`-only.
- **`ValidateMessage(schema_or_name, encoding, message)`** — pre-flight check that a candidate payload would be accepted. Useful in unit tests for asserting "this struct serializes to something the schema accepts" without a real publish round-trip.
- **`ListSchemas` / `ListSchemaRevisions`** — paginated listings, with `view=FULL` (gcloud `--view=FULL`) returning the full proto text. Useful for audit/inventory tooling.
- **`GetSchema(name@revisionId)`** — fetch a specific historical revision by appending `@<8-char-id>` to the schema name. Handy when investigating a stamped revision on an old message.
- **`RollbackSchema(schemaID, revisionID)`** — commits a new revision identical to a named older one. Prefer this over `DeleteSchemaRevision` for "undo" — it preserves history and avoids stranding consumers that still reference the bad revision.

---

## 7. Quick reference

### Required IAM

| Role | Used by |
|---|---|
| `roles/pubsub.editor` | Schema/topic/subscription admin tooling |
| `roles/pubsub.publisher` | Runtime publishers |
| `roles/pubsub.subscriber` | Runtime consumers |

Granular permissions if you cannot use `editor`: `pubsub.schemas.{create,commit,delete,get,list,listRevisions,rollback,attach,validate}`.

### Reserved message attributes

| Key | Set by | Used by |
|---|---|---|
| `googclient_schemaname` | server, on validated publish | optional — audit/logging |
| `googclient_schemaencoding` | server, on validated publish | consumer — drives unmarshal selection |
| `googclient_schemarevisionid` | server, on validated publish | consumer — log for observability |

### gRPC error codes that matter

| Code | Where | Meaning |
|---|---|---|
| `AlreadyExists` | `CreateSchema` / `CreateTopic` / `CreateSubscription` | Resource already present — treat as success in idempotent setup |
| `NotFound` | `Delete*` / `Get*` | Resource gone — usually safe to ignore in teardown |
| `InvalidArgument` | `Publish` | Payload doesn't match any allowed revision in the topic's range |
| `InvalidArgument` | `CommitSchema` | New revision is not backward-compatible with prior revision |

### Quotas and constants

- 20 revisions per schema
- 300 KB per schema definition
- 10,000 schemas per project
- 8-character auto-generated revision IDs
- A few minutes of propagation lag on revision and revision-range changes

### Reference links

Official Google Cloud Pub/Sub schema documentation:

- Overview — https://docs.cloud.google.com/pubsub/docs/schemas
- Create schemas — https://docs.cloud.google.com/pubsub/docs/create-schemas
- Commit a schema revision — https://docs.cloud.google.com/pubsub/docs/commit-schema-revision
- Delete a schema — https://docs.cloud.google.com/pubsub/docs/delete-schema
- Delete a schema revision — https://docs.cloud.google.com/pubsub/docs/delete-schema-revision
- List schemas — https://docs.cloud.google.com/pubsub/docs/list-schemas
- List schema revisions — https://docs.cloud.google.com/pubsub/docs/list-schema-revisions
- Roll back a schema — https://docs.cloud.google.com/pubsub/docs/roll-back-schemas
- View schema details — https://docs.cloud.google.com/pubsub/docs/view-schema-details
- Validate a schema definition — https://docs.cloud.google.com/pubsub/docs/validate-schema-definition
- Validate a message against a schema — https://docs.cloud.google.com/pubsub/docs/validate-schema-message
- Associate a schema with a topic — https://docs.cloud.google.com/pubsub/docs/associate-schema-topic
- Publish to a schema-bound topic — https://docs.cloud.google.com/pubsub/docs/publish-topics-schema
