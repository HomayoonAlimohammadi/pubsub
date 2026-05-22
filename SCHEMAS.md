# Pub/Sub Schemas with Protobuf — How It Works, How This Repo Implements It, and What to Watch For

This document explains how Google Cloud Pub/Sub validates and evolves message schemas when the schema type is **Protocol Buffers**, and how this repo wires that flow up with the v2 Go client. It is the canonical reference for the demo in `cmd/demo` and is meant to be read top-to-bottom by anyone touching schema/topic/subscription admin or message encoding in this codebase.

If you only want the "what does the code do" tour, jump to [Implementation tour](#implementation-tour). If you want to understand *why* the code is shaped that way, read the concept sections first.

---

## 1. Concept model

### 1.1 What a Pub/Sub schema is

A Pub/Sub schema is a **server-side resource** (`projects/{p}/schemas/{id}`) that stores a single proto file's text as the contract for a topic's payloads. It is independent of any topic — you create the schema first, then attach it to one or more topics through their `SchemaSettings`. After attachment, every `Publish` request to that topic is validated against the schema before Pub/Sub accepts it; non-conforming payloads are rejected with `INVALID_ARGUMENT` and never reach a subscription.

Two schema **types** are supported: `AVRO` and `PROTOCOL_BUFFER`. This repo uses `PROTOCOL_BUFFER` exclusively (see `internal/admin/admin.go:32`).

### 1.2 Revisions

Each schema is a stack of immutable **revisions**. The first revision is created by `CreateSchema`; subsequent revisions are added by `CommitSchema`. Each revision gets an auto-generated 8-character ID (e.g. `a1b2c3d4`) and is stamped on every message that validates against it (see [Attributes the server adds](#15-attributes-the-server-adds)).

Hard limits to remember:

| Limit | Value |
|---|---|
| Revisions per schema | 20 (delete an old one to add a new one beyond this) |
| Schema definition size | 300 KB |
| Schemas per project | 10,000 |

A topic doesn't bind to a single revision — it binds to a **range** (`first_revision_id`, `last_revision_id`). When the range is left open (both unset), the topic accepts the oldest existing revision through the newest one as new ones are committed. This is the mode the demo uses, and it's what makes "commit a v2 and publish against it immediately" work without touching the topic.

### 1.3 Encoding (BINARY vs JSON)

A topic's `SchemaSettings.Encoding` declares what the *wire format* of `Message.Data` must be:

- `BINARY` — `proto.Marshal` output. Compact, no field-name overhead, what the demo uses.
- `JSON` — `protojson.Marshal` output. Human-readable, larger, slightly different validation semantics (Avro JSON has union encoding quirks; for proto3, JSON is largely equivalent to BINARY).

Encoding is **fixed per topic** (changing it on a live topic breaks active publishers). Publishers must pick the right marshaler; consumers must pick the right unmarshaler. Both sides discover the encoding differently:

- Publishers: by reading `topic.SchemaSettings.Encoding` **once at startup** and caching it (`internal/publisher/publisher.go:28-44`).
- Consumers: from the per-message attribute `googclient_schemaencoding` (`internal/consumer/consumer.go:38-46`).

### 1.4 Validation at publish time

When a publish lands at a schema-bound topic:

1. Pub/Sub iterates the topic's allowed revision range **newest-first**.
2. For each revision, it tries to parse `Message.Data` in the topic's encoding against that revision's proto definition.
3. The **first revision that parses successfully wins** — the message is accepted and stamped with that revision's ID.
4. If no revision matches, the publish fails synchronously with gRPC `INVALID_ARGUMENT` (the Go SDK surfaces this from `result.Get(ctx)`; see `internal/publisher/publisher.go:64-69`).

The "newest-first" matching has two consequences worth internalizing:

- **A v1 message also validates against v2** if v2 is a superset (e.g. added optional `population`). The server stops at v2 and stamps v2. The consumer therefore can't infer "the publisher knew about v1" from the revision ID — only "the message is at least v1-compatible *and* the newest matching revision is v2."
- **A v2 message containing the new field will not validate against v1.** That's the whole point of revisions: clients that upgrade can use new fields safely.

### 1.5 Attributes the server adds

On every successfully validated publish, Pub/Sub stamps three reserved attributes onto the message:

| Attribute key | Value |
|---|---|
| `googclient_schemaname` | Full schema resource name (`projects/{p}/schemas/{id}`) |
| `googclient_schemarevisionid` | 8-char revision ID that matched |
| `googclient_schemaencoding` | `BINARY` or `JSON` |

These are reserved keys — applications must not write to them and should not rely on absence. Consumers should treat the encoding attribute as the **only** source of truth about how to decode the payload. `GetTopic` from a consumer is the wrong tool: the topic's current settings can be edited out from under in-flight messages, and consumers should remain valid for any past revision.

---

## 2. Schema evolution: what you can and cannot change

Pub/Sub enforces a **compatibility check** on `CommitSchema`. For `PROTOCOL_BUFFER` schemas the rule is narrow: *you may add or remove `optional` fields, and nothing else*. Concretely:

| Change to `.proto` | Accepted? | Notes |
|---|---|---|
| Add a field declared `optional` with a new tag | yes | The intended path for additive evolution. |
| Remove a field that was declared `optional` | yes | Old payloads with the field still decode (they keep an unknown field). |
| Add a non-`optional` / `repeated` / nested-message field | no | Even though proto3 singular fields look additive, the server rejects this. |
| Remove a non-`optional` field | no | |
| Rename a field (same tag, new name) | no | |
| Change a field's type | no | |
| Change a field's tag number | no | |

**The `optional` keyword in proto3 is load-bearing.** Without it, a singular field has *implicit presence* — proto3's default behavior — and Pub/Sub treats it as non-optional for compatibility purposes. This is the single sharpest edge in the whole system; do not skip it on any field you might ever need to remove. See the [drift note](#7-known-drift-in-this-repo) for how the current `proto/state.proto` is out of step with this.

If a commit fails, the SDK returns gRPC `INVALID_ARGUMENT` from `SchemaClient.CommitSchema` — this is `codes.InvalidArgument`, distinct from `AlreadyExists` and `NotFound`. The demo doesn't have a dedicated negative-path command; you can reproduce one by editing `proto/state.proto` to rename `post_abbr`, running `make proto`, and re-running `bin/demo commit ...`.

### 2.1 Other operations on the schema stack

- **Rollback** (`SchemaClient.RollbackSchema`) does *not* revert state — it **creates a new revision** whose definition is a copy of an older one. Intervening revisions stay in the history; if any messages were validated against them, the revision ID stamped on those messages still resolves. Treat rollback as "publish a hotfix revision identical to the last known good", not as "rewind".
- **Deleting a single revision** (`DeleteSchemaRevision`) is allowed as long as the schema has more than one revision. If a topic's revision range includes the deleted ID, validation simply skips it (the first/last pointers shift to the surviving neighbor as needed). Messages already stamped with the deleted revision keep that revision ID in their attributes, but downstream consumers that try to `GetSchema(...@revision)` for it will get `NOT_FOUND`. Notably, **BigQuery subscriptions with schema validation enabled will fail to write** messages whose validating revision has been deleted — prefer rollback over delete-revision in production.
- **Deleting the entire schema** (`DeleteSchema`) is destructive: all attempts to publish to topics bound to it begin failing immediately. You cannot delete a schema that is still bound to a topic without first either removing the binding (topic update) or deleting the topic — the demo's `Teardown` deletes sub, then topic, then schema in that order for this reason (`internal/admin/admin.go:120-142`).
- **Validating a definition before commit** (`ValidateSchema`) lets you check that a proto file parses, *but does not check compatibility with prior revisions* — that gate is only enforced inside `CommitSchema`.
- **Validating a message before publish** (`ValidateMessage`) takes either a schema name or an inline definition plus an encoding, and tells you whether a candidate payload would be accepted. Useful for client-side dry-runs and tests.

---

## 3. Lifecycle of a typed topic (end to end)

The flow this demo exercises:

```
proto/state.proto  --(CreateSchema)-->  schema (revision r1)
                                              |
                                              v
                          (CreateTopic with SchemaSettings)
                                              |
                                              v
                                          topic ---(CreateSubscription)---> subscription
                                              |
                                              v
                              Publish(BINARY proto.Marshal(State{...}))
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
  new publishes carrying the new field validate against r2 (newest-first)
  old publishes omitting the field still validate against r2 (newest-first); a v1 with no field also stamps r2
```

There is no client-side schema fetch in the message path. The publisher learns the encoding once at startup. The consumer learns the encoding (and revision ID) from per-message attributes. The schema resource itself is consulted only by `CreateSchema`/`CommitSchema`/admin tooling.

---

## 4. Implementation tour

Module layout:

```
proto/state.proto                       # source of truth — what gets uploaded as the schema
gen/statepb/                            # protoc output, committed
internal/admin/admin.go                 # all admin RPCs (schema, topic, sub lifecycle)
internal/publisher/publisher.go         # caches topic encoding, marshals, publishes
internal/consumer/consumer.go           # Receive loop, attribute-driven decode
cmd/demo/main.go                        # subcommand CLI: setup | publish | consume | commit | teardown
Makefile                                # `make proto` runs protoc and stages the .pb.go
```

Module path: `github.com/HomayoonAlimohammadi/pubsub`. The v2 client (`cloud.google.com/go/pubsub/v2`) is used throughout — v1 imports are not present and should not be added.

### 4.1 Admin — `internal/admin/admin.go`

The admin layer is a thin shell over the v2 client's three admin surfaces:

- `client.SchemaClient` for schema CRUD and `CommitSchema` (note: in this repo it's invoked via `pubsubapiv1.NewSchemaClient(ctx)` directly rather than `client.SchemaClient`, see `admin.go:22`).
- `client.TopicAdminClient` for topic CRUD (`admin.go:77`).
- `client.SubscriptionAdminClient` for subscription CRUD (`admin.go:98`).

Functions to know:

- `CreateSchema(ctx, projectID, schemaID, protoPath)` (`admin.go:16`) — reads the `.proto` file from disk and uploads its bytes as `Schema.Definition` with `Type: PROTOCOL_BUFFER`. Returns the first revision ID.
- `CommitRevision(ctx, projectID, schemaID, protoPath)` (`admin.go:43`) — same payload shape, but as `CommitSchemaRequest` against an existing schema. Returns the new revision ID. Server-side compatibility check runs here; surface `INVALID_ARGUMENT` to the user verbatim.
- `CreateTopicWithSchema(ctx, projectID, topicID, schemaID)` (`admin.go:70`) — creates a topic with `SchemaSettings.Schema` set to the full schema resource name, encoding hard-coded to `BINARY`, and `first/last_revision_id` deliberately left unset so the topic accepts any committed revision.
- `CreateSubscription(ctx, projectID, topicID, subID)` (`admin.go:91`) — plain subscription bound to the topic, ack deadline 30s, no DLQ.
- `Teardown(ctx, ...)` (`admin.go:111`) — deletes sub → topic → schema, joining errors with `errors.Join` and tolerating `NOT_FOUND`. Order matters: a schema bound to a topic cannot be deleted.
- `IsAlreadyExists(err)` (`admin.go:147`) — exported helper so the CLI can treat re-setup as a no-op (`cmd/demo/main.go:62,70,78`).

A small wart worth knowing: each admin function calls `pubsub.NewClient` (or `pubsubapiv1.NewSchemaClient`) fresh and closes it on return. That's fine for one-shot CLI commands but would be wasteful in a long-lived process — share a single `*pubsub.Client` across operations there.

### 4.2 Publisher — `internal/publisher/publisher.go`

`Publisher` holds three things: the v2 client, a `*pubsub.Publisher` for the topic, and a cached encoding.

- `New(ctx, projectID, topicID)` (`publisher.go:22`) calls `GetTopic` **once** to read `SchemaSettings.Encoding`. If `SchemaSettings` is nil, it errors out — a topic without a schema is not the intended use of this codebase. The encoding is then cached on the struct.
- `Publish(ctx, *statepb.State)` (`publisher.go:47`) switches on the cached encoding (`Encoding_JSON` → `protojson.Marshal`, else `proto.Marshal`), publishes, and calls `result.Get(ctx)` to surface server validation failures synchronously. `codes.InvalidArgument` is wrapped with a clear message so it's distinguishable from transient errors.
- `Close()` (`publisher.go:75`) calls `pub.Stop()` first to flush in-flight batches, then closes the client. Skipping `Stop()` will silently drop messages still in the batcher.

Why cache encoding instead of fetching per publish: `GetTopic` is an admin RPC, much heavier than `Publish`, and the encoding never changes for the life of a process. One fetch at startup is sufficient.

### 4.3 Consumer — `internal/consumer/consumer.go`

`Consumer` is intentionally minimal: a `*pubsub.Client` and a `*pubsub.Subscriber`. `Run(ctx, h)` (`consumer.go:33`) installs a callback on `Subscriber.Receive` that:

1. Reads `msg.Attributes["googclient_schemaencoding"]` to pick `proto.Unmarshal` vs `protojson.Unmarshal`. The default branch falls back to binary and logs a warning — see [Considerations](#5-considerations--gotchas) for why a strict failure would be more honest.
2. On decode error, logs the offending `googclient_schemarevisionid` and `Nack`s.
3. Calls the user handler with the decoded `*statepb.State` plus the full attribute map (so handlers can log the revision ID for verification). Handler errors → `Nack`; success → `Ack`.

The consumer never calls `GetTopic` and never holds a schema client. All the schema knowledge it needs travels with each message.

### 4.4 CLI — `cmd/demo/main.go`

Subcommands map 1:1 to admin/publisher/consumer entry points:

| Command | Function | What it does |
|---|---|---|
| `setup` | `runSetup` (`main.go:45`) | `CreateSchema` + `CreateTopicWithSchema` + `CreateSubscription`, treating `ALREADY_EXISTS` as success |
| `publish` | `runPublish` (`main.go:89`) | Builds a `State` and publishes once; prints the returned message ID |
| `consume` | `runConsume` (`main.go:133`) | Runs `Consumer.Run` until `--duration` elapses; logs decoded `State` plus the revision attribute |
| `commit` | `runCommit` (`main.go:176`) | Reads the current `proto/state.proto` and calls `CommitRevision`; prints the new revision ID |
| `teardown` | `runTeardown` (`main.go:199`) | Calls `admin.Teardown` |

The CLI is single-shot — every command opens fresh clients and exits. No daemonization, no cross-command state.

---

## 5. Considerations & gotchas

The patterns below are the ones a senior engineer will burn time on if they're not already aware. They apply regardless of whether you keep the demo's CLI shape or wrap these packages into a service.

**`optional` is mandatory for any field you might ever remove.** Pub/Sub's compatibility check treats proto3 singular fields as non-optional and rejects their removal in future revisions. If you commit `int64 population = 3;` today, that field is permanent — you can never delete it via `CommitSchema`, only by deleting the entire schema (which breaks all publishers). Add `optional` defensively on every new field unless you are certain it is forever.

**The proto file must be self-contained.** No `import` statements, exactly one top-level message, no references to types from other files. The schema definition stored server-side is the raw file text; the server parses it standalone. `make proto` only runs `protoc` on the single file (`Makefile:3-8`), which is consistent with this constraint.

**Encoding is fixed; changing it breaks live publishers.** This demo hard-codes `BINARY` (`admin.go:81`). If you ever flip a topic to `JSON`, any publisher still marshaling with `proto.Marshal` will start failing schema validation with `INVALID_ARGUMENT`. The encoding cache in `Publisher` (`publisher.go:43`) makes this even worse for long-lived processes — they won't see the change without a restart.

**The consumer's `default:` branch in the encoding switch should be a hard failure, not a warning.** Today (`consumer.go:43-46`) an unknown encoding attribute falls through to `proto.Unmarshal` with a `fmt.Println`. If Pub/Sub ever introduces a third encoding, or if a non-schema message somehow lands on this subscription, the consumer will mis-decode and `Ack` garbage. Prefer returning an error and `Nack`ing, or panic-on-impossible-state if you'd rather find out loudly.

**`CreateSchema`, `CreateTopic`, and `CreateSubscription` are not idempotent server-side** — they return `ALREADY_EXISTS`. The CLI handles this in `runSetup` (`main.go:62,70,78`), but any other caller must do the same. `admin.IsAlreadyExists` (`admin.go:147`) is the canonical check.

**Publisher must call `Stop()` before exit.** The v2 `Publisher` batches under the hood; closing the client without stopping the publisher drops in-flight messages on the floor. `Publisher.Close` (`publisher.go:75-85`) does the right thing in the right order: `pub.Stop()` first, then `client.Close()`. Mirror this if you ever construct a publisher directly.

**Revision matching is newest-first.** A v1 payload that omits the v2 optional field will still match v2 (the field is optional, so absent is valid) and the message will be stamped with v2's ID — not v1's. Don't use the revision attribute as a proxy for "which client version produced this"; use a separate explicit field for that if you need it.

**Topics created with an open revision range pick up new revisions automatically.** This is what makes "commit a revision and immediately publish against it" work without a topic update. The flip side: any revision committed to that schema becomes valid on the topic the moment it is committed; revisions are not opt-in per topic. If you need stricter control, set `first_revision_id`/`last_revision_id` explicitly on the topic — but then you must update the topic every time you commit, which mostly defeats the point.

**Schema revision IDs are 8-character UUIDs.** They are visible only in API responses and the `googclient_schemarevisionid` attribute. Log them on consume — they are the only durable handle you have for forensic questions like "which schema did this message validate against?"

**There is a propagation lag for revision and revision-range changes** — typically a few minutes. Tests that commit and immediately publish a message exercising the new field can be flaky; either retry or accept that "instant" only means single-digit minutes in practice.

**Auth is ADC.** The code does not accept a key-file path anywhere, and shouldn't. Make sure `gcloud auth application-default login` has been run or that the runtime has a workload identity attached. The acting principal needs `roles/pubsub.editor` for admin operations and `roles/pubsub.publisher` + `roles/pubsub.subscriber` for the runtime path.

**Don't read schema/topic metadata from the consumer path.** Consumers must rely on `googclient_schemaencoding` and `googclient_schemarevisionid` only. Calling `GetTopic` or `GetSchema` from the receive loop adds latency, costs admin RPC budget, and races against admin edits.

---

## 6. Operational extras (not currently in the CLI, but you'll want them)

- **`ValidateSchema(definition)`** — pre-flight check that a `.proto` file parses server-side. Doesn't enforce compatibility against prior revisions. A natural addition for a `bin/demo validate-def` subcommand or a pre-commit Git hook.
- **`ValidateMessage(schema_or_name, encoding, message)`** — pre-flight check that a candidate payload would be accepted. Good for unit tests where you want to assert "this struct serializes to something the live schema accepts" without round-tripping through a real publish.
- **`ListSchemas` / `ListSchemaRevisions`** — paginated listings. `--view=FULL` (gcloud) or the `FULL` view in the API returns the full `.proto` text; basic view returns only metadata. Useful for audit tooling.
- **`GetSchema(name@revision)`** — fetch a specific historical revision by appending `@<revisionId>` to the schema name. Handy when investigating a stamped revision on an old message.
- **`RollbackSchema(schemaID, revisionID)`** — commits a new revision identical to the named older one. Use this in preference to deleting revisions when you need to "undo" a bad commit; deletion can leave BigQuery subscriptions stranded on messages that no longer have a resolvable revision.

---

## 7. Known drift in this repo

Two things in the current tree diverge from the HANDOFF's intent and from Pub/Sub's documented rules:

1. **`proto/state.proto:8` declares `int64 population = 3;` without `optional`.** HANDOFF Task 8a is explicit that this must be `optional int64 population = 3;`, and the compatibility rules say only `optional` fields can be added or removed in future revisions. As written, the schema is functional but **`population` can never be removed via a future `CommitSchema`** — only by deleting the entire schema. If this is a real GCP project's live schema, consider whether you want to fix it now (delete + recreate) before more code or downstream consumers depend on the current shape.

2. **`cmd/demo/main.go:222-233` (`resolveProjectID`) hard-codes a project ID and has the `GCP_PROJECT_ID` env-var fallback commented out.** The unit tests in `cmd/demo/main_test.go` still assume the env-var path works, so `go test ./cmd/demo` will fail. Decide whether the demo is now single-project-specific (and update or delete the tests) or restore the env-var fallback.

3. Minor: `internal/consumer/consumer.go:44` uses `fmt.Println("invalid schema encoding??: ...")` for the default branch. Either upgrade to `log.Printf` and `Nack`, or remove the question marks — the rest of the file uses `log`.

None of these block the demo from running against a real GCP project; they are the things to fix before this code is reused as a template.

---

## 8. Quick reference

### Required IAM

| Role | Used by |
|---|---|
| `roles/pubsub.editor` | Anything in `internal/admin` |
| `roles/pubsub.publisher` | `internal/publisher` (runtime) |
| `roles/pubsub.subscriber` | `internal/consumer` (runtime) |

Granular permissions if you can't use `editor`: `pubsub.schemas.{create,commit,delete,get,list,listRevisions,rollback,attach,validate}`.

### Reserved message attributes

| Key | Where set | Where read |
|---|---|---|
| `googclient_schemaname` | server, on accept | optional — for audit/logging |
| `googclient_schemaencoding` | server, on accept | `internal/consumer/consumer.go:38` — drives decode |
| `googclient_schemarevisionid` | server, on accept | `internal/consumer/consumer.go:48,165` — log for verification |

### gRPC error codes you'll actually see

| Code | Meaning in this codebase |
|---|---|
| `AlreadyExists` | `CreateSchema`/`CreateTopic`/`CreateSubscription` against existing resource — treat as success in setup |
| `NotFound` | `Teardown` on already-deleted resource — ignore |
| `InvalidArgument` (publish) | Schema validation failed — payload doesn't match any allowed revision |
| `InvalidArgument` (commit) | New revision is not backward-compatible with the prior one |

### Reference links

Pub/Sub schema docs (all read for this writeup):

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
