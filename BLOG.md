# Building AI-Native Infrastructure with Specmatic: How I Eliminated Integration Uncertainty in a Multi-Service System

*A hands-on walkthrough of contract-first development, async event contracts, and building the feedback loops that AI coding agents actually need.*

---

## Introduction

There's a gap between what AI coding agents can generate and what production systems actually need.

An agent can write a Flask route. It can write an OpenAPI spec. It can even write tests. But when you have two services built by two different agents — or two different humans — that need to talk to each other, the integration is where everything silently breaks.

I built **specmatic-taskflow** to explore this exact problem. It's a multi-service task management system where a Task Service, a User Service, a Kafka event bus, and a Kanban frontend all talk to each other. But more importantly, I integrated **Specmatic** into every layer — turning OpenAPI and AsyncAPI specifications into living, executable tests that both humans *and* AI coding agents can use as a feedback signal.

This post is a walkthrough of what I built, how I designed the contract-first workflow, and what I learned about making contracts enforceable rather than aspirational.

---

## The Problem: Integration Uncertainty at Scale

When you build a single-service application, failures are visible. When you build a multi-service system, failures are *silent* until production.

Here's the typical failure mode:

1. Service A and Service B both implement the same OpenAPI spec.
2. A developer changes a field name in Service A (`dueDate` → `due_date`).
3. The OpenAPI spec isn't updated. Tests pass because they're mocked.
4. Service B still expects `dueDate`. Everything looks fine in staging.
5. Production breaks.

This problem gets *worse* with AI coding agents. When you ask an LLM to implement a service, it generates code based on its training — not based on what the *other* services in your system expect. Without a feedback mechanism that can tell an AI "your implementation doesn't match the contract," you get plausible-looking code that doesn't integrate.

The solution isn't more documentation. It's **executable contracts**.

---

## What I Built: specmatic-taskflow

specmatic-taskflow is an AI-native task management system with:

- **Task Service** — Flask REST API (CRUD on tasks, publishes to Kafka)
- **User Service** — Flask REST API (user registry, assignee lookup)
- **Apache Kafka** — Event bus for task lifecycle events (`task-created`, `task-updated`)
- **Kanban Frontend** — Vanilla JavaScript SPA with no build toolchain overhead
- **Specmatic Contract Tests** — REST tests (OpenAPI 3.0.1) + Async tests (AsyncAPI 3.0.0)
- **Schema Resiliency Tests** — Auto-generated positive variations beyond hand-written examples
- **Specmatic Mock Server** — Serves contract-driven mock responses before real services exist
- **Specmatic Studio** — Visual IDE for contract exploration

Everything runs via Docker Compose. Bringing up the full stack also runs contract tests — so integration verification is not a separate step, it's built into the startup.

```
specmatic-taskflow/
├── specs/
│   ├── openapi/
│   │   ├── task-api.yaml          # Task Service contract
│   │   └── user-api.yaml          # User Service contract
│   └── asyncapi/
│       ├── task-events.yaml       # Kafka event contract
│       └── examples/              # Before-hooks that trigger publishes via the REST API
├── services/
│   ├── task-service/              # Flask + Kafka publisher
│   └── user-service/              # Flask user registry
├── frontend/                      # Vanilla JS Kanban board
├── specmatic.yaml                 # Task API REST + async Kafka test configuration
├── specmatic-user.yaml            # User API REST test configuration
└── docker-compose.yaml            # Full orchestration
```

---

## Step 1: Write the Contract First

The first principle of this project: **the spec exists before the code.**

I wrote `task-api.yaml` before writing a single line of Flask. Every endpoint, every status code, every field name was decided in the OpenAPI document. The implementation's job was to satisfy this contract, not to define it.

Here's a simplified view of the Task API contract:

```yaml
# specs/openapi/task-api.yaml
openapi: 3.0.1
info:
  title: Task Service API
  version: 1.0.0

paths:
  /tasks:
    get:
      summary: List all tasks
      parameters:
        - name: status
          in: query
          schema:
            type: string
            enum: [pending, in-progress, completed]
          examples:
            TASKS_200_OK:
              value: pending
      responses:
        '200':
          content:
            application/json:
              examples:
                TASKS_200_OK:
                  value:
                    - id: 1
                      title: "Implement Login Feature"
                      status: "in-progress"
                      priority: "high"

    post:
      summary: Create a task
      requestBody:
        content:
          application/json:
            examples:
              CREATE_TASK_201:
                value:
                  title: "Write Unit Tests"
                  priority: "medium"
              CREATE_TASK_400:
                value:
                  priority: "medium"   # Missing required 'title'
      responses:
        '201':
          content:
            application/json:
              examples:
                CREATE_TASK_201:
                  value:
                    id: 100
                    title: "Write Unit Tests"
                    status: "pending"
                    priority: "medium"
        '400':
          content:
            application/json:
              examples:
                CREATE_TASK_400:
                  value:
                    error: "Validation failed"
                    message: "title is required"
```

The critical detail: **inline example names must match across parameters, request bodies, and responses.**

When Specmatic sees `CREATE_TASK_400` in both the request body and the 400 response, it pairs them into a single test scenario: "send this request body, expect this response." This is how examples become test cases without writing any test code.

The naming convention I used — `TASKS_200_OK`, `CREATE_TASK_201`, `CREATE_TASK_400`, `GET_TASK_404` — is the entire test suite. No test files. No mocking frameworks. No setup/teardown.

---

## Step 2: Run the Contract, Watch It Fail

Before writing any Flask code, I ran Specmatic against a stub service.

```bash
docker compose up test-task-api
```

Specmatic hit the (non-existent) endpoints and failed every test. But this failure is meaningful — it's a precise diff between what the contract expects and what the service provides. This is exactly the kind of signal that an AI coding agent can act on.

The `specmatic.yaml` configuration:

```yaml
version: 3

systemUnderTest:
  service:
    definitions:
      - definition:
          source:
            filesystem:
              directory: specs/openapi
          specs:
            - task-api.yaml
    runOptions:
      openapi:
        type: test
        baseUrl: http://task-service:8080

specmatic:
  settings:
    test:
      schemaResiliencyTests: all
  governance:
    report:
      formats:
        - html
      outputDirectory: build/reports/specmatic
    successCriteria:
      minCoveragePercentage: 100
      maxMissedOperationsInSpec: 0
      enforce: true
  license:
    path: /specmatic/specmatic-license.txt
```

Three governance settings matter here:

- `minCoveragePercentage: 100` — Every single endpoint in the spec must be tested. No skipping.
- `maxMissedOperationsInSpec: 0` — If the service has endpoints not in the spec, it fails.
- `schemaResiliencyTests: all` — Beyond the hand-written examples, Specmatic automatically generates every valid positive schema variation *and* every invalid (negative) variation — wrong types, broken enums, missing required fields. More on this in Step 4.

This makes the contract **enforcing**, not advisory.

---

## Step 3: Build the Service to Satisfy the Contract

With the failing tests as my guide, I implemented the Task Service:

```python
# services/task-service/main.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import json, os

app = Flask(__name__)
CORS(app)

# Three tasks seeded with dedicated IDs — one per mutating operation
# to prevent test ordering conflicts (see Challenges section).
tasks = {
    1: {"id": 1, "title": "Implement Login Feature", "status": "in-progress",
        "assignee": "alice", "priority": "high", "createdAt": "2024-01-15T10:00:00Z"},
    2: {"id": 2, "title": "Write Unit Tests", "status": "pending",
        "assignee": "bob", "priority": "medium", "createdAt": "2024-01-15T11:00:00Z"},
    3: {"id": 3, "title": "Review Pull Request", "status": "pending",
        "assignee": "alice", "priority": "low", "createdAt": "2024-01-15T12:00:00Z"},
}
_next_id = 100  # avoids collision with seed IDs during schema resiliency POST tests

@app.route("/actuator/health", methods=["GET"])
def actuator_health():
    return jsonify({"status": "UP"})

@app.route('/tasks', methods=['GET'])
def list_tasks():
    status_filter = request.args.get('status')
    result = list(tasks.values())
    if status_filter:
        result = [t for t in result if t['status'] == status_filter]
    return jsonify(result), 200

@app.route('/tasks', methods=['POST'])
def create_task():
    global _next_id
    data = request.get_json(silent=True) or {}
    if not data.get('title'):
        return jsonify({"error": "Validation failed", "message": "title is required"}), 400
    if not data.get('priority'):
        return jsonify({"error": "Validation failed", "message": "priority is required"}), 400

    task = {
        "id": _next_id,
        "title": data['title'],
        "description": data.get('description', ''),
        "status": "pending",
        "assignee": data.get('assignee', ''),
        "priority": data['priority'],
        "createdAt": _now(),
    }
    tasks[_next_id] = task
    _next_id += 1

    _publish("task-created", {
        "taskId": task["id"],
        "title": task["title"],
        "status": task["status"],
        "assignee": task["assignee"],
        "priority": task["priority"],
        "timestamp": task["createdAt"],
    })

    return jsonify(task), 201
```

The implementation's shape was dictated by the contract. Field names, status codes, error response format — all copied from the spec, not invented.

After implementing all five endpoints (`GET /tasks`, `POST /tasks`, `GET /tasks/{taskId}`, `PUT /tasks/{taskId}`, `DELETE /tasks/{taskId}`), I ran the contract tests again:

```bash
docker compose up --exit-code-from test-task-api test-task-api
# Exit code: 0 ✓
```

All tests pass. Not because I wrote tests against my own implementation — but because my implementation satisfies a contract that was written independently.

---

## Step 4: Schema Resiliency — Beyond Hand-Written Examples

One example per endpoint is a floor, not a ceiling.

With `schemaResiliencyTests` in `specmatic.yaml`, Specmatic automatically expands each example into every variation the schema allows. No extra test files. No test authoring. The schema itself defines the test space. There are three modes:

| Mode | What it generates |
|------|--------------------|
| `positiveOnly` | Every *valid* combination of enum values and optional-field presence |
| `negativeOnly` | Every *invalid* combination — wrong types, broken enums, missing required fields |
| `all` | Both — the full positive and negative test space |

Here's what this means in practice for `PUT /tasks/{taskId}`:

```yaml
UpdateTaskRequest:
  type: object
  properties:
    status:
      type: string
      enum: [pending, in-progress, completed]   # 3 values
    assignee:
      type: string
    priority:
      type: string
      enum: [low, medium, high]                 # 3 values
```

All three fields are optional. The hand-written example only covers `{"status": "completed", "assignee": "alice"}`. On the positive side, Specmatic generates every valid combination of enum values and optional field presence. On the negative side, it also throws `assignee: 42`, `priority: "urgent"`, `status: null`, and dozens of similar mutations at the same endpoint — each one expecting a `4xx` response back.

I run with `schemaResiliencyTests: all`. The test count jump is real:

| Mode | Total tests (task-api) | Total tests (user-api) |
|------|------------------------|-------------------------|
| Example-only (no resiliency) | ~10 | ~5 |
| `positiveOnly` | 27 | — |
| `all` (positive + negative) | 135 | 49 |

This catches two different classes of bugs:

- **Positive resiliency**: a service that handles `priority: "high"` but crashes on `priority: "low"` — something a single hand-written example would never reveal.
- **Negative resiliency**: a service that *should* reject `priority: "urgent"` or `assignee: true` with a `400`, but instead silently accepts it and corrupts its own state — something no hand-written example was ever written to catch, because nobody writes examples for requests they don't expect.

**One important consequence:** more tests means more mutations, which means test execution order matters. I cover how I solved this in the Challenges section.

---

## Turning On `schemaResiliencyTests: all` — and Fixing What It Found

Flipping `positiveOnly` to `all` didn't just add more passing tests — it took the task-api suite from 27 green tests to **133 tests, 96 of them failing**. Here's the real output from that first run:

```
Tests run: 133, Successes: 37, Failures: 96, WIP: 0, Errors: 0
```

That's not noise. It's every place where the Flask services accepted garbage instead of rejecting it. Three distinct failure patterns showed up.

### Failure 1 — No type or enum validation on write endpoints

`POST /tasks` and `PUT /tasks/{taskId}` only checked that required fields were *truthy* — they never checked that `title` was a string, that `priority` was one of `low|medium|high`, or that `assignee` wasn't a boolean. Specmatic's negative generator throws every wrong type at every field:

```
-ve  Scenario: POST /tasks -> 4xx with the request from the example 'CREATE_TASK_201'
     where REQUEST.BODY contains all the keys AND the key priority is mutated
     from ("low" or "medium" or "high") to number FAILED
Reason:
    >> RESPONSE.STATUS
        Expected 4xx status, but received 201
```

87 of the 96 failures were exactly this shape: `Expected 4xx status, but received 200/201`. The fix was real input validation in both services — type checks via `isinstance`, enum checks against an explicit set, applied *before* anything gets written to the in-memory store:

```python
# services/task-service/main.py
TASK_PRIORITIES = {"low", "medium", "high"}
TASK_STATUSES = {"pending", "in-progress", "completed"}

def create_task():
    data = request.get_json(silent=True) or {}

    title = data.get("title")
    if not isinstance(title, str) or not title:
        return _validation_error("title is required")

    priority = data.get("priority")
    if priority not in TASK_PRIORITIES:
        return _validation_error(f"priority must be one of {sorted(TASK_PRIORITIES)}")

    if "description" in data and not isinstance(data["description"], str):
        return _validation_error("description must be a string")
    if "assignee" in data and not isinstance(data["assignee"], str):
        return _validation_error("assignee must be a string")
    ...
```

The same pattern applied to `PUT /tasks/{taskId}` (status/priority enums, assignee type) and to `user-service`'s `POST /users` (role enum). One subtlety that cost a second round of failures: checking `value is not None and not isinstance(value, str)` lets an explicit `null` slip through, because `None is not None` is `False`. The fix is to drop the `is not None` guard entirely — if the key is present, its value must satisfy the type, full stop.

### Failure 2 — The contract had no `400` to validate against

After adding validation, a different error appeared for `PUT /tasks/{taskId}`:

```
Reason:
    >> RESPONSE.STATUS
        R0002: HTTP status mismatch
        Specification expected status 404 but response contained status 400
```

And for `GET /tasks?status=<invalid>`:

```
Reason:
    >> RESPONSE.STATUS
        Received 400, but the specification does not contain a 4xx or default
        response, hence unable to verify this response
```

The service was now doing the *right* thing — returning `400` for bad input — but the OpenAPI spec never declared a `400` response for these two operations. Only `POST /tasks` had one. Specmatic enforces the contract literally: if you return a status code the spec doesn't document, that's a contract violation, even if it's semantically correct. This is the contract-first principle paying for itself — the gap was in the spec, not just the code. The fix was adding the missing response definitions to `task-api.yaml`:

```yaml
# GET /tasks
'400':
  description: Validation error — invalid status filter
  content:
    application/json:
      schema:
        $ref: '#/components/schemas/Error'
      examples:
        LIST_TASKS_400:
          value:
            error: "Validation failed"
            message: "status must be one of ['completed', 'in-progress', 'pending']"

# PUT /tasks/{taskId}
'400':
  description: Validation error — invalid field value
  content:
    application/json:
      schema:
        $ref: '#/components/schemas/Error'
      examples:
        UPDATE_TASK_400:
          value:
            error: "Validation failed"
            message: "priority must be one of ['high', 'low', 'medium']"
```

Each new `400` needed a matching named example on the parameter/request side too (`LIST_TASKS_400` on the `status` query param, `UPDATE_TASK_400` on both the `taskId` path param and the request body) — Specmatic pairs examples by name, the same rule from Challenge 1.

### Failure 3 — Flask's default 404 page broke the error schema

Mutating `taskId` from a number to a string or boolean (e.g. `DELETE /tasks/true`) doesn't match Flask's `<int:task_id>` route converter, so Flask falls back to its own default 404 — an HTML page, not JSON:

```
>> RESPONSE.HEADER.Content-Type
    R1002: Value mismatch
    Specification expected application/json but response contained text/html; charset=utf-8

>> RESPONSE.BODY
    R1001: Type mismatch
    Specification expected type json object but response contained value
    "<!doctype html>\n<html lang=en>\n<title>404 Not Found</title>..."
```

The fix is a Flask-wide 404 handler that returns JSON matching the `Error` schema for any path Flask itself can't route — independent of the explicit `404` responses already returned by `get_task`/`update_task`/`delete_task` when a *valid* taskId doesn't exist:

```python
@app.errorhandler(404)
def handle_not_found(_exc):
    return jsonify({"error": "Not Found", "message": "The requested resource was not found"}), 404
```

### Result

After all three fixes — input validation, the missing `400` contract responses, and the JSON 404 handler — task-api went from **37/133 passing to 135/135 passing** (two new explicit `400` examples added two tests), and user-api went from **30/49 to 49/49**. Both numbers were stable across repeated runs, confirming the fixes weren't masking flakiness.

```
Tests run: 135, Successes: 135, Failures: 0, WIP: 0, Errors: 0
Tests run: 49, Successes: 49, Failures: 0, WIP: 0, Errors: 0
```

The headline: **96 negative-path bugs existed in services that had 100% positive-path contract coverage and a green CI pipeline.** Schema resiliency in `all` mode is the difference between "the API works when you use it correctly" and "the API is actually safe to expose to a client you don't control" — which, for any service called by another team, another service, or an AI agent generating its own request bodies, is the only question that matters.

---

## Step 5: Extend to Async Events with AsyncAPI

REST APIs are only half the story in an event-driven system. When the Task Service creates a task, it publishes to Kafka. Without a contract for those messages, any consumer can break silently.

I wrote an AsyncAPI 3.0.0 spec for the Kafka events:

```yaml
# specs/asyncapi/task-events.yaml
asyncapi: 3.0.0
info:
  title: Task Events
  version: 1.0.0

servers:
  kafka:
    host: kafka:9092
    protocol: kafka

channels:
  task-created:
    address: task-created
    messages:
      TaskCreatedMessage:
        payload:
          type: object
          required: [taskId, title, status, timestamp]
          properties:
            taskId:
              type: integer
            title:
              type: string
            status:
              type: string
              enum: [pending, in-progress, completed]
            timestamp:
              type: string
              format: date-time
        examples:
          - payload:
              taskId: 1
              title: "New deployment task"
              status: pending
              timestamp: "2024-01-15T10:00:00Z"
```

The async contract test works differently from REST tests. Instead of Specmatic acting as a client hitting endpoints, the `test-async` process:

1. Subscribes to the Kafka `task-created` and `task-updated` topics
2. Runs a `before` hook that issues a real HTTP call against the Task Service (e.g. `POST /tasks`)
3. Validates the message that publish produces against the AsyncAPI schema

```yaml
# specmatic-async.yaml
version: 3

systemUnderTest:
  service:
    definitions:
      - definition:
          source:
            filesystem:
              directory: specs/asyncapi
          specs:
            - task-events.yaml
    runOptions:
      asyncapi:
        type: test
        subscriberReadinessWaitTime: 5000
        servers:
          - host: kafka:9092
            protocol: kafka
    data:
      examples:
        - directories:
            - specs/asyncapi/examples
```

> **Gotcha I hit:** The AsyncAPI spec initially had `host: localhost:9092`. Inside Docker's network, services communicate by service name, not localhost. Changing to `host: kafka:9092` (matching the Docker Compose service name) fixed the connection immediately. Always verify your spec's server hosts match your runtime network topology.

> **Gotcha I hit (the expensive one):** Inline `examples` on an AsyncAPI Message are documentation only — nothing actually calls the REST API to make the service publish. I initially put the example payload directly on the `operations` entry, which AsyncAPI 3.0 rejects (`examples` is only valid on `Message`, not `Operation`). Moving it to the message fixed parsing, but the test still timed out waiting for a Kafka message, because there was still nothing driving the publish. The actual mechanism is external JSON example files (`specs/asyncapi/examples/`, wired up via `data.examples.directories`) with a `before` array that fires the real HTTP request:
>
> ```json
> {
>   "name": "TASK_CREATED_EXAMPLE",
>   "before": [
>     {
>       "type": "http",
>       "http-request": { "baseUrl": "http://task-service:8080", "path": "/tasks", "method": "POST", "body": { "title": "...", "priority": "high" } },
>       "http-response": { "status": 201 }
>     }
>   ],
>   "send": { "topic": "task-created", "payload": { "taskId": "(integer)", "title": "...", "timestamp": "(datetime)" } }
> }
> ```
>
> `(integer)` / `(datetime)` are Specmatic's type-matcher placeholders — useful here since the published `taskId` and `timestamp` are server-generated and can't be hardcoded.

> **Gotcha I hit (CI-only):** `specmatic test --config=specmatic-async.yaml` quietly does nothing for AsyncAPI — `--config` only applies to the OpenAPI test path. Since the whole repo (including the real `specmatic.yaml`) is bind-mounted into every Specmatic container, the async test was silently running the REST config and reporting "no test scenarios found." Fixed by bind-mounting `specmatic-async.yaml` over `/usr/src/app/specmatic.yaml` for the `test-async` service specifically, so its default config lookup resolves correctly without touching the other services.

> **Gotcha I hit (the one that mattered most):** Once the wiring was right, the test still failed — every message timed out. The task service's `_publish()` wraps the Kafka call in a `try/except` (so the service can start before Kafka is ready), and that swallowed an exception every single time: `kafka-python==2.0.2` doesn't work on Python 3.12 (`No module named 'kafka.vendor.six.moves'`). The Kafka publish had never actually succeeded — it just looked fine because nothing was asserting on it until the async contract test existed. Switching to `kafka-python-ng` (a maintained drop-in fork, same `import kafka`) fixed it. This is exactly the failure mode contract testing is supposed to catch: a swallowed exception that made the service *look* healthy while silently dropping every event.

> **Update — the standalone config didn't survive:** everything above was true when it was written, but `specmatic-async.yaml` and the separate `test-async` container are gone now. Task API (OpenAPI) and task-events (AsyncAPI) both test the same `task-service`, so there was no real reason to keep them on two separate `systemUnderTest.service` definitions bind-mounted into two separate containers. I merged the AsyncAPI `definition`/`runOptions.asyncapi` block straight into `specmatic.yaml` alongside the existing OpenAPI one, deleted `specmatic-async.yaml`, and collapsed `test-async` into `test-task-api` — one `specmatic test` run now reports REST and Kafka results together (135/135 REST + 2/2 async, 100% coverage), and the `--profile async` flag doesn't exist anymore. Everything else in this section — the `kafka:9092` host fix, the external example files with `before` hooks, the `kafka-python-ng` swap — is still exactly how it works today; only the file/container boundary changed.

---

## Step 6: Mock Server for Parallel Development

While building the frontend Kanban board, the Task Service wasn't fully implemented yet. Rather than hardcoding fake data or building a separate mock, I used Specmatic's mock server:

```bash
docker compose --profile mock up mock-task-api
```

Specmatic reads `task-api.yaml`, extracts the inline examples, and serves them as HTTP responses — with zero additional configuration. `GET /tasks` returns the `TASKS_200_OK` example. `POST /tasks` with a missing title returns the `CREATE_TASK_400` example.

The frontend has a built-in API switcher in the sidebar that toggles between the mock server (`:9100`) and the real service (`:8080`). A frontend developer can work against realistic API responses before the backend exists, and switching to the real service requires changing exactly one URL.

This is the pattern that enables **parallel development without integration ceremonies**: frontend, backend, and mobile teams can all work simultaneously against the same source of truth (the spec), without coordinating implementation timelines.

---

## Step 7: Governance Enforcement in CI

The final piece is making contracts enforced — not just available.

The `docker-compose.yaml` uses `--exit-code-from` to make the entire compose stack fail if contract tests fail:

```bash
# In a CI pipeline
docker compose up --exit-code-from test-task-api
echo "Exit code: $?"
```

With `minCoveragePercentage: 100`, a developer can't ship a partial implementation. If the spec defines `DELETE /tasks/{taskId}` and the service doesn't implement it, the entire CI run fails.

Because `schemaResiliencyTests: all` lives in `specmatic.yaml` rather than as a CLI flag, every CI run picks it up automatically — `docker compose run test-task-api` and `docker compose run test-user-api` both read the same config, so the GitHub Actions workflow (`.github/workflows/ci.yml`) enforces negative-path validation on every push and pull request with zero pipeline changes beyond the config file itself. There's no separate "resiliency" job to maintain or forget to run — it's the same job, just testing a larger space.

Specmatic generates HTML reports at `build/reports/specmatic/test/html/index.html`. These show exactly which scenarios passed, which failed, and the diff between expected and actual responses — the artifact that goes into pull request reviews.

The governance model I enforced:

| Rule | Effect |
|------|--------|
| Every operation in the spec = tested | Can't skip endpoints |
| Every response = validated against schema | Field drift caught immediately |
| Any drift between spec and implementation | Build failure |

This turns the OpenAPI spec from passive documentation into an active quality gate.

---

## Step 8: Authentication & Authorization with Multiple OpenAPI Security Schemes

Contract testing without auth testing has a blind spot: a service can satisfy every schema and still leak data to anyone who asks. I closed that gap by borrowing the pattern from the neighboring `labs/api-security-schemes` lab — vary the security scheme by HTTP method, then let Specmatic prove the service actually enforces whichever one is declared.

`task-api.yaml` ended up with three schemes on one spec:

```yaml
components:
  securitySchemes:
    basicAuth:
      type: http
      scheme: basic          # GET — any valid account may read
    oAuth2AuthCode:
      type: oauth2            # POST/PUT — developer, manager, or admin role
      flows:
        authorizationCode:
          authorizationUrl: http://localhost:8083/realms/taskflow/protocol/openid-connect/auth
          tokenUrl: http://localhost:8083/realms/taskflow/protocol/openid-connect/token
    apiKeyAuth:
      type: apiKey
      in: header
      name: X-API-Key         # DELETE — admin role only
```

`user-api.yaml` uses one scheme (`apiKeyAuth`) everywhere, with an elevated-role check bolted onto `POST /users` only.

The POST/PUT scheme started life as a static `bearerAuth` (`type: http, scheme: bearer`) checked against a hardcoded token dictionary — no different in substance from the API key, just a different header name. Gotcha 3 below covers why and how it became a real Keycloak-backed `oAuth2AuthCode` scheme instead. Both Flask services enforce authorization with small decorator functions (`require_basic_auth`, `require_bearer_auth(allowed_roles)`, `require_api_key(allowed_roles)`) backed by a demo identity table — four accounts (`alice`/developer, `bob`/qa, `diana`/manager, `charlie`/admin). A decorator returns `401` for a missing/invalid credential and `403` for a valid credential whose role isn't in the allowlist; here's the current, JWT-validating version of the bearer decorator:

```python
def require_bearer_auth(allowed_roles):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            auth_header = request.headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                return _unauthorized("Bearer token required")
            token = auth_header[len("Bearer "):].strip()
            try:
                signing_key = _jwks_client.get_signing_key_from_jwt(token)
                claims = jwt.decode(
                    token, signing_key.key, algorithms=["RS256"],
                    options={"verify_aud": False, "verify_iss": False},
                )
            except Exception:
                return _unauthorized("Invalid or expired token")
            token_roles = set(claims.get("realm_access", {}).get("roles", []))
            role = next((r for r in ("developer", "qa", "manager", "admin") if r in token_roles), None)
            if not role:
                return _unauthorized("Token does not carry a recognized role")
            if role not in allowed_roles:
                return _forbidden(f"role '{role}' is not permitted to perform this action")
            return f(*args, **kwargs)
        return wrapper
    return decorator
```

### Gotcha 1: `securitySchemes` only work through `systemUnderTest`, not the ad hoc CLI form

This one took real digging through `specmatic-schema.json` to resolve. Before this change, `test-task-api` and `test-user-api` both ran `specmatic test <spec-file> --testBaseURL=...` — a positional-file invocation. That form clearly ignores `systemUnderTest.service` entirely: it was already working for `user-api.yaml` even though the `specmatic.yaml` in the repo only ever declared `task-api.yaml` under `systemUnderTest`. Top-level `specmatic:` settings (`governance`, `schemaResiliencyTests`, `license`) are read regardless of invocation style, but the schema shows `securitySchemes` lives three levels deeper — `systemUnderTest.service.runOptions.openapi.specs[].spec.securitySchemes` — a path that's only consulted when Specmatic resolves the spec *through* `systemUnderTest`, not via a bare file argument.

So adding `security:` to the specs and a `securitySchemes` block to `specmatic.yaml` would have been silently ignored by the existing docker-compose commands. The fix: switch both test containers to the bare `specmatic test` form (same as `test-async` already used), letting `systemUnderTest.service.definitions` supply the spec and `runOptions.openapi.baseUrl` supply the target. Since `systemUnderTest.service` is singular, the User API needed its own `specmatic-user.yaml` — bind-mounted over `/usr/src/app/specmatic.yaml` for that one container, the exact trick already in place for `specmatic-async.yaml`.

I validated both new config files against `specmatic-schema.json` with `jsonschema.validate()` before touching docker-compose — it confirmed the shape was structurally legal, but that turned out not to be the whole story.

### Gotcha 2: schema-valid isn't the same as working — `securitySchemes` also needs a matching `id`

The Specmatic Enterprise license had lapsed while I was building this, so the first real Docker run only happened after it was renewed. The first attempt still failed: every positive scenario came back `401`, including plain `GET /tasks`. Grepping the test output for `Authorization` showed why — Specmatic was sending fuzzed, auto-generated credentials (`Authorization: Bearer molestiae`, `Authorization: Basic RUpTWE46TFVUSk4=`) instead of the ones in `specmatic.yaml`. My `runOptions.openapi.specs[]` entry had no `id`, and neither did the `definitions.specs` entry it was supposed to override — with nothing to correlate them, Specmatic silently fell back to generated values instead of raising an error. The fix, once I looked back at how the reference lab does it, was to give both sides a matching `id` (`taskApiSpec` / `userApiSpec`):

```yaml
systemUnderTest:
  service:
    definitions:
      - definition:
          specs:
            - spec:
                id: taskApiSpec
                path: task-api.yaml
    runOptions:
      openapi:
        specs:
          - spec:
              id: taskApiSpec
              securitySchemes: { ... }
```

That got every positive scenario back to `200`/`201`/`204` — but the run still exited non-zero. Coverage had dropped to 58%, because the coverage report tracks every response code declared in an operation's `responses:` map as its own gradable unit, and the `401`/`403` responses I'd added (schema-only, no examples) showed up as "not tested" — I'd assumed a contract-test run could only ever attach one configured credential per scheme, so those codes could never actually be triggered. First instinct was to just drop the declarations and lower `minCoveragePercentage` to the resulting ceiling (58%/56%) — CI stayed green, but it was a workaround for a problem I hadn't actually diagnosed correctly.

That assumption turned out to be wrong, and a founder's review of this submission is what surfaced it: the reference [`api-security-schemes`](../api-security-schemes) lab's actual contract (pulled from `specmatic/labs-contracts` at test time, so not visible in this repo's checkout of that lab) has an `auth_examples/` directory full of dedicated external example files — `post-orders_application_json_401.json` hardcodes a bad bearer token directly in the request; `post-orders-create-forbidden.json` uses a `before` fixture to log into Keycloak as a real, valid, wrong-role user. Each example carries its **own** credential, completely independent of the one global credential configured for the positive-path scenarios — so each one executes for real, hits the server, and counts as tested coverage. The "only one credential per run" limitation was a limitation of how I'd wired the global `securitySchemes` override, not of Specmatic itself.

I added the equivalent here — `specs/openapi/examples/task-api/` and `specs/openapi/examples/user-api/`, wired in via `data.examples.directories` — the same mechanism already used for the async Kafka `before`-hooks. For `basicAuth`/`apiKeyAuth` this was simple: one gotcha surfaced — simply omitting the auth header in an example doesn't produce `401`, Specmatic still auto-attaches the globally configured *valid* credential to fill the gap, so the fix is to set the header to some other explicit (invalid) value, which takes precedence. The `bearerAuth`/OAuth2 case needed a much bigger, separate fix — see Gotcha 3. With everything in place, both specs measure genuine **100% coverage with every test passing** — no lowered ceiling required. Both bugs (the missing `id` and the coverage cost) only showed up once I actually ran the containers — `jsonschema.validate()` against the config schema and `openapi-spec-validator` against the specs both passed the whole time, because the files were legal, just not correctly *wired* or *exercised*.

### Gotcha 3: migrating to real Keycloak OAuth2 — three failed assumptions before the actual fix

Testing 401/403 "for real" worked cleanly for the static schemes, but `bearerAuth` was itself just a static pre-shared string checked against a dictionary — not meaningfully different from the API key, and not actually testing OAuth2. Migrating it to a real Keycloak-backed `oAuth2AuthCode` scheme (a new `keycloak` service in `docker-compose.yaml`, a realm export at `keycloak/taskflow-realm.json`) surfaced a chain of problems, each only visible by running the stack:

**No curl in the Keycloak image, and the health endpoint isn't on the port you'd guess.** `quay.io/keycloak/keycloak` ships no `curl`/`wget`. Keycloak 26 also serves `/health/ready` on the *management* port (9000), not the main port (8080) I first pointed the healthcheck at. Fixed with a `CMD-SHELL` healthcheck using bash's built-in `/dev/tcp` to speak raw HTTP directly — no external tool needed.

**"Account is not fully set up" on login, and it's not about required actions.** Every imported user had `requiredActions: []` explicitly — confirmed via Keycloak's own admin REST API, not just the realm file. The real cause: Keycloak 26's declarative User Profile marks `firstName`/`lastName` required by default, and `VERIFY_PROFILE` gets computed dynamically at login for any user missing them, regardless of what's in `requiredActions`. Fixed by adding `firstName`/`lastName` to every user in the realm export.

**The same token has a different `iss` claim depending on how you asked for it.** Keycloak derives the issuer from the request's own Host header, so a token fetched from the browser (`localhost:8083`) and one fetched from inside Docker (`keycloak:8080`) get different `iss` values for the same realm. Pinning it via `KC_HOSTNAME`/`KC_HOSTNAME_PORT` didn't produce the value I expected either. Since JWKS signature verification already proves a token came from this exact Keycloak instance's private key, I disabled issuer and audience verification in Flask (`verify_iss: False`, `verify_aud: False`) instead of chasing a fragile pin — a deliberate, documented simplification for a single-realm demo.

**There's no way to forward a captured value into a later request within a Specmatic example — at least not in this version.** The natural design: a `before` fixture logs into Keycloak, captures `"(ACCESS_TOKEN:string)"`, the next request references `$(ACCESS_TOKEN)`. I tried the reference lab's exact pattern (no explicit header, relying on undocumented auto-binding) — checked byte-for-byte, their files never reference the captured variable anywhere — and it just fell back to the global credential. I then tried the documented capture/reference syntax explicitly: the literal string `$(ACCESS_TOKEN)` was sent verbatim, completely unsubstituted. Tested in both the `before`+`partial` REST format and the sequential `before` array the async hooks use — identical result both times, confirmed with real Docker output, not assumed.

**The actual fix lives in the test container's entrypoint, not in the example files.** `test-task-api` in `docker-compose.yaml` now copies the (read-only-mounted) repo into a scratch directory, fetches real tokens for alice (developer) and bob (qa) straight from Keycloak, and `sed`-substitutes them into the two examples and two async hooks that need a *specific* role. Everything else that just needs *some* valid allowed-role credential has no explicit `Authorization` header at all and picks up the freshly-fetched developer token from the global config automatically — same mechanism as `basicAuth`/`apiKeyAuth`. One last `cp` gotcha: Docker auto-creates `working_dir` as an empty directory before the entrypoint runs, so `cp -r /src /tmp/app` copies `/src` as a *subdirectory* of the already-existing `/tmp/app` rather than populating it — `cp -r /src/. /tmp/app/` was the fix.

### Gotcha 4: the token fetch itself was racing Keycloak's realm import

Even after Gotcha 3 was fixed, the suite was still sporadically flaky — not every run, just occasionally a `401` on an async before-fixture or on the `403` QA example, with no code change in between runs to explain it.

The cause was a race, not an auth bug: `test-task-api` depends on Keycloak via `service_healthy`, but that healthcheck only proves `/health/ready` is up — it says nothing about whether `--import-realm` has actually finished registering the `task-api` client and its users yet. The very first password-grant request could land in that gap and get an error response back, and because the entrypoint script didn't have `set -o pipefail`, a failed `curl` piped into `jq` doesn't fail the pipeline — it just produces an empty string, which then got `sed`-substituted into the example files as if it were a real token:

```bash
# Before — one-shot, no protection against losing the realm-import race
DEV_TOKEN=$(curl -sf -X POST "$KEYCLOAK_BASE_URL/realms/taskflow/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=task-api&username=$KEYCLOAK_DEV_USERNAME&password=$KEYCLOAK_DEV_PASSWORD" | jq -r .access_token)
QA_TOKEN=$(curl -sf -X POST "$KEYCLOAK_BASE_URL/realms/taskflow/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=task-api&username=$KEYCLOAK_QA_USERNAME&password=$KEYCLOAK_QA_PASSWORD" | jq -r .access_token)
```

```bash
# After — pipefail on, retry until a real JWT comes back or fail loudly
set -o pipefail

fetch_token() {
  local username="$1" password="$2" attempt token
  for attempt in $(seq 1 15); do
    token=$(curl -sf -X POST "$KEYCLOAK_BASE_URL/realms/taskflow/protocol/openid-connect/token" \
      -d "grant_type=password&client_id=task-api&username=$username&password=$password" | jq -r '.access_token // empty')
    if [ -n "$token" ]; then
      echo "$token"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: failed to fetch Keycloak token for $username after 15 attempts" >&2
  return 1
}

DEV_TOKEN=$(fetch_token "$KEYCLOAK_DEV_USERNAME" "$KEYCLOAK_DEV_PASSWORD")
QA_TOKEN=$(fetch_token "$KEYCLOAK_QA_USERNAME" "$KEYCLOAK_QA_PASSWORD")
```

`jq -r '.access_token // empty'` turns a missing field into an explicit empty string rather than the literal text `null`, so the `-n "$token"` check works correctly. The retry loop gives realm import up to 30 seconds to finish before giving up, and `pipefail` means any other kind of curl failure surfaces as a real script error instead of quietly propagating an empty token downstream. Same class of bug as the swallowed Kafka exception back in Step 5 — a failure that looked like test flakiness but was actually a docker-compose dependency graph that didn't model the thing it needed to wait for.

### What Specmatic tests here — authentication and authorization, both for real

Running the suite with the securitySchemes correctly wired re-validates every existing example and schema-resiliency scenario against an authenticated backend — `basicAuth`/`apiKeyAuth` via a static admin (`charlie`) credential, `oAuth2AuthCode` via a real token fetched from Keycloak for alice (developer) at the start of every run — verified live: `test-task-api` 143/143 REST scenarios plus 2/2 async Kafka scenarios in the same run, both at 100% coverage; `test-user-api` 53/53 at 100%. Break the credential (wrong Keycloak password, or a valid-but-wrong-role account) and every scenario relying on that global credential fails — the same signal the reference lab demonstrates.

Authorization is tested the same way, not just declared: the dedicated external examples assert `bob` (qa) gets `403` on `POST /tasks` while the positive-path examples assert `alice` (developer) gets `201` — in the *same* single contract-test run, because each example brings its own credential rather than relying on the one global value. OpenAPI's `security` keyword only models *authentication* (who you are) on its own; the per-role authorization rule ("qa can authenticate but can't create tasks") is real, enforced application logic in the Flask services, and I verified it twice — once as a counted Specmatic scenario, and once live via curl:

```bash
QA_TOKEN=$(curl -s -X POST http://localhost:8083/realms/taskflow/protocol/openid-connect/token \
  -d "grant_type=password&client_id=task-api&username=bob&password=password234" | jq -r .access_token)
curl -i -X POST http://localhost:8080/tasks \
  -H "Authorization: Bearer $QA_TOKEN" \
  -H "Content-Type: application/json" -d '{"title":"x","priority":"low"}'
# 403 Forbidden — qa role not permitted

DEV_TOKEN=$(curl -s -X POST http://localhost:8083/realms/taskflow/protocol/openid-connect/token \
  -d "grant_type=password&client_id=task-api&username=alice&password=password123" | jq -r .access_token)
curl -X POST http://localhost:8080/tasks \
  -H "Authorization: Bearer $DEV_TOKEN" \
  -H "Content-Type: application/json" -d '{"title":"x","priority":"low"}'
# 201 Created — developer role permitted
```

I verified the full matrix (401/403/200/201/204 across every operation and every role) by running both Flask services directly and driving them with `curl`.

Two smaller downstream fixes came out of turning this on:
- The async `before`-hooks (`specs/asyncapi/examples/*.json`) call `POST /tasks` and `PUT /tasks/2` directly — they now carry a placeholder `Authorization` header that the test entrypoint substitutes with a real fetched token, or `test-task-api` (which runs the async contract test alongside REST, see the update in Step 5) would start failing its setup calls with `401`.
- The frontend's "Real Service" target hits `task-service:8080` directly from the browser, so it now fetches its own token from Keycloak on first use (password grant, cached) — clearly commented as a lab/demo shortcut, not a real login flow.

---

## The AI-Native Angle: Building Feedback Loops for Coding Agents

Here's why this architecture matters beyond traditional development.

When you ask an AI coding agent to implement a service, the typical workflow is:

1. Give the agent a task description
2. Agent generates code
3. Human manually checks if it's right

With contract-driven development, the workflow becomes:

1. Write (or generate) the OpenAPI spec
2. Give the agent the spec + the failing Specmatic output
3. Agent generates code
4. Specmatic runs and produces a precise diff
5. Feed the diff back to the agent
6. Agent iterates until all tests pass
7. No human verification needed at the integration layer

The contract becomes the **objective ground truth** that the AI optimizes against. It's not "does this code look right" — it's "does this code satisfy these concrete, machine-verifiable assertions?"

This is what "AI-native infrastructure" actually means: not using AI to write code, but building the verification substrate that lets AI code generation be *reliable at scale*.

---

## Full System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Compose                       │
│                                                          │
│  ┌──────────────┐      ┌──────────────┐                 │
│  │ task-service │      │ user-service │                 │
│  │  Flask :8080 │      │  Flask :8081 │                 │
│  └──────┬───────┘      └──────────────┘                 │
│         │ publishes events                               │
│  ┌──────▼───────┐                                       │
│  │    Kafka     │                                       │
│  │    :9092     │                                       │
│  └──────────────┘                                       │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │  test-task-api                                      │ │
│  │  Specmatic REST + AsyncAPI                          │ │
│  │  (OpenAPI 3.0.1 + Kafka topic tests, one run)        │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌──────────────┐   ┌─────────────────────────────────┐ │
│  │  frontend    │   │  mock-task-api                  │ │
│  │  Kanban:3000 │   │  Specmatic Mock Server :9100    │ │
│  └──────────────┘   └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

---

## Contract Coverage at a Glance

| Spec File | Endpoints / Channels | Named Examples | External 401/403 Examples | Tests with `schemaResiliencyTests: all` |
|-----------|---------------------|----------------|----------------------------|------------------------------------------|
| `specs/openapi/task-api.yaml` | 5 REST endpoints | 12 (200, 201, 204, 400, 404 scenarios) | 8 (`specs/openapi/examples/task-api/`) | 143 |
| `specs/openapi/user-api.yaml` | 3 REST endpoints | 5 (201, 400, 200, 200, 404) | 4 (`specs/openapi/examples/user-api/`) | 53 |
| `specs/asyncapi/task-events.yaml` | 2 Kafka channels | `task-created`, `task-updated` | — |

---

## Real Errors Specmatic Surfaced — and How I Fixed Them

To demonstrate Specmatic's feedback loop concretely, I deliberately introduced a field rename in `task-service/main.py` — the kind of silent drift that kills multi-service systems in production — and ran the full contract test suite to capture what Specmatic actually reports.

### The break: `title` → `task_title`

A single-line change in the `create_task` handler:

```python
# Before (matches the spec)
task = {
    "id": _next_id,
    "title": data["title"],
    ...
}

# After (drifted from the spec)
task = {
    "id": _next_id,
    "task_title": data["title"],   # ← renamed
    ...
}
```

### What Specmatic reported

Running `docker compose up --exit-code-from test-task-api`:

**Failure 1 — POST /tasks → 500 instead of 201**

The service crashed immediately. The Kafka publish code still referenced `task["title"]`, which no longer existed:

```
taskflow-task-service | ERROR in app: Exception on /tasks [POST]
taskflow-task-service |   File "/app/main.py", line 106, in create_task
taskflow-task-service |     "title": task["title"],
taskflow-task-service |              ~~~~^^^^^^^^^
taskflow-task-service | KeyError: 'title'
taskflow-task-service | 172.19.0.2 - - [POST /tasks] 500

taskflow-test-task-api | +ve  Scenario: POST /tasks -> 201 ... has FAILED
taskflow-test-task-api | Reason:
taskflow-test-task-api |   >> RESPONSE.STATUS
taskflow-test-task-api |       R0002: HTTP status mismatch
taskflow-test-task-api |       Specification expected status 201 but response contained status 500
```

All 6 schema resiliency variants of `POST /tasks → 201` failed the same way.

**Failure 2 — GET /tasks → schema violation (cascade)**

Because the task was inserted into the in-memory store *before* the Kafka publish crashed, the malformed object (with `task_title` instead of `title`) persisted. When `GET /tasks` ran next, the list response contained those corrupt items — and Specmatic caught it at the schema layer:

```
taskflow-test-task-api | +ve  Scenario: GET /tasks -> 200 ... has FAILED
taskflow-test-task-api | Reason:
taskflow-test-task-api |   >> RESPONSE.BODY[3].title
taskflow-test-task-api |       R2001: Missing required property
taskflow-test-task-api |       Specification expected mandatory property "title" to be present
taskflow-test-task-api |       but was missing from the response
taskflow-test-task-api |
taskflow-test-task-api |   >> RESPONSE.BODY[3].task_title
taskflow-test-task-api |       R2003: Unknown property
taskflow-test-task-api |       Property "task_title" in the response was not in the specification
```

**Final result:**

```
Tests run: 27, Successes: 19, Failures: 8, WIP: 0, Errors: 0
89% API Coverage — FAILED (minCoveragePercentage = 100%)
```

One field rename caused **8 failures across two endpoints** — 6 from POST crashing and 2 from GET returning corrupt data — before a single human reviewer saw anything.

### The fix

Two things needed to be consistent:

```python
# 1. Keep the task dict key matching the spec field name
task = {
    "id": _next_id,
    "title": data["title"],   # ← back to "title"
    ...
}

# 2. The Kafka publish code already referenced task["title"] correctly
#    — so reverting the dict key was the only change needed
```

After reverting: **27/27 tests passing, 100% coverage.**

### What this demonstrates

The contract test caught the break at the *exact* moment it was introduced — no integration environment, no manual testing, no waiting for another team to hit the issue. The error output told me precisely which field was wrong (`RESPONSE.BODY[3].title`), which rule was violated (`R2001`, `R2003`), and what the spec expected versus what the service returned.

This is the feedback loop that makes contract-first development reliable at scale.

---

## Challenges and What I Learned

### 1. Example naming symmetry is non-negotiable

Specmatic matches examples across request/response pairs by name. If your parameter example is named `GET_TASK_200` but your response example is `GET_TASK_SUCCESS`, Specmatic won't pair them into a test. I standardized on a `VERB_RESOURCE_STATUSCODE` convention and never had pairing issues after that.

### 2. Healthcheck timing in Docker Compose

The `service_completed_successfully` dependency type waits for a container to exit — it doesn't mean the service inside is *ready*. I added explicit healthchecks to every service and used `service_healthy` dependencies for the test containers, eliminating "connection refused" flakiness:

```yaml
healthcheck:
  test: ["CMD", "python", "-c",
    "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"]
  interval: 2s
  timeout: 5s
  retries: 30
  start_period: 5s
```

### 3. Kafka publish blocking service startup

The first implementation blocked Flask startup while connecting to Kafka. If Kafka wasn't ready, the entire service failed. I wrapped the Kafka producer and every publish call in try/except:

```python
def _publish(topic: str, payload: dict):
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(topic, payload)
        producer.flush(timeout=3)
    except Exception as exc:
        app.logger.warning("Kafka publish skipped (%s): %s", topic, exc)
```

The service stays healthy and responds to REST calls even if Kafka is temporarily unavailable.

### 4. AsyncAPI server host resolution

The AsyncAPI spec's `servers[].host` must match the hostname that the Specmatic container can resolve. Inside Docker Compose, that's the service name (`kafka`), not `localhost`. Always check your spec's server host against your runtime network topology.

### 5. Specmatic Enterprise requires `/actuator/health` to execute tests

This one cost me time. Running `docker compose up test-user-api` produced an HTML report showing **0% coverage, 4 tests skipped, "Actuator Not Available"** — even though the Flask service was fully up and responding correctly.

Specmatic Enterprise checks for a `GET /actuator/health` endpoint before executing any tests. Without it, every scenario is marked "Skipped" regardless of service health. The fix is a single route on each Flask service:

```python
@app.route("/actuator/health", methods=["GET"])
def actuator_health():
    return jsonify({"status": "UP"})
```

This is separate from the application's own `/health` endpoint. Once added, all 4 tests ran and coverage showed correctly.

### 6. Schema resiliency + stateful in-memory store = test ordering trap

Enabling `schemaResiliencyTests: positiveOnly` expanded the task-api suite from ~10 to 27 tests. More tests exposed a subtle ordering bug I hadn't hit before.

Specmatic runs operations on the same path alphabetically by HTTP method: **DELETE → GET → PUT**. My spec used `taskId=1` for all three success scenarios. When DELETE ran first and removed task 1, the subsequent GET and all 11 PUT schema resiliency variations against `taskId=1` returned 404 instead of 200 — 13 cascading failures from a single deletion.

The fix: assign each mutating operation its own dedicated seed task.

```python
tasks = {
    1: {...},   # GET /tasks/1  — read-only, never touched by other operations
    2: {...},   # PUT /tasks/2  — mutated but never deleted
    3: {...},   # DELETE /tasks/3 — disposable
}
_next_id = 100  # POST schema resiliency tests create IDs 100+ — no collision with seeds
```

And in the spec, update the examples to match:

```yaml
GET_TASK_200:   value: 1   # task 1
UPDATE_TASK_200: value: 2  # task 2
DELETE_TASK_204: value: 3  # task 3
```

Each operation now has its own data slice. Tests pass regardless of execution order.

### 7. `schemaResiliencyTests: all` requires the spec and the service to grow together

Switching from `positiveOnly` to `all` wasn't just a config flag — every negative test that found a missing `400` response meant the *spec* was incomplete, not just the code. I'd documented `400` for `POST /tasks` and `POST /users` from day one (because "what if a required field is missing" is an obvious case to write an example for), but `GET /tasks?status=` and `PUT /tasks/{taskId}` had no documented error path for invalid input, because nobody had written a negative example for them. Negative resiliency testing finds exactly the blind spots that example-driven testing structurally can't — the requests nobody thought to write down. The fix pattern is always the same: add the `4xx` response to the spec, add a validation check to the service that returns it, in that order.

### 8. `securitySchemes` needs `systemUnderTest`, not ad hoc CLI args — and authorization isn't authentication

Covered in full in Step 8, but the short version: Specmatic only reads `runOptions.openapi.specs[].spec.securitySchemes` when it resolves a spec through `systemUnderTest.service` (not a bare `specmatic test <file> --testBaseURL=...` invocation), and even then it only applies them once the `definitions.specs` entry and the `runOptions` override share an explicit matching `id` — omit it and Specmatic silently falls back to fuzzed, auto-generated credentials with no error. Declaring `401`/`403` as formal `responses` entries only costs coverage if nothing ever triggers them — my first pass relied solely on the one global credential, so they sat permanently "not tested." Dedicated external examples (`specs/openapi/examples/`), each carrying its own hardcoded or wrong-role credential independent of the global config, fix that: they execute for real and get counted, taking both specs to genuine 100% coverage. That same mechanism also closes the authorization gap — OpenAPI's `security` keyword only models authentication on its own, but an example with `bob`'s real qa-role token asserting `403` tests per-role authorization as a real, counted scenario too, not just something verified separately via curl. All of this — the `id` requirement, the coverage cost, and the fix — only became fully visible once the Enterprise license was renewed and the suite actually ran in Docker; schema/spec validation alone had passed the whole time.

---

## Running It Yourself

```bash
git clone https://github.com/kanugurajesh/Spectamic-Taskflow
cd Spectamic-Taskflow

# Copy your Specmatic Enterprise license into the project root first:
# cp /path/to/your/license.txt ./license.txt

# Full stack + REST contract tests (use --build on first run)
docker compose up --build

# Run only the contract tests (REST + async Kafka events, one run)
docker compose up test-task-api test-user-api --build --abort-on-container-exit

# Frontend against mock server (no backend needed)
docker compose --profile mock up mock-task-api frontend

# Open Specmatic Studio (visual contract editor)
docker compose --profile studio up studio
# Visit http://localhost:9000

# CI mode: fail fast on contract violations
docker compose up --exit-code-from test-task-api

# Tear down
docker compose down --remove-orphans
```

Test reports are generated at `build/reports/specmatic/test/html/index.html`.

> **Note:** Always pass `--build` after changing any service code (`services/*/main.py`). Spec file changes (`.yaml`) are picked up immediately via volume mount — no rebuild needed for those.

---

## Conclusion

The most important thing I learned building specmatic-taskflow: **a contract that isn't executable isn't a contract, it's a suggestion.**

OpenAPI specs are everywhere. AsyncAPI specs are increasingly common. But most of them live as YAML files in a `/docs` folder that nobody updates after the initial commit. They become lies — specifications that describe a system as it was, not as it is.

Specmatic changes the relationship. When your spec *runs* — when it becomes the test, the mock, the governance gate — it has to stay true. Every CI run verifies it. Every developer who breaks it sees it in the build log.

For AI coding agents, this matters even more. An agent that generates code against a contract and gets precise, executable feedback can iterate reliably. An agent working against documentation can only guess.

I built specmatic-taskflow to prove that a single engineer can, in a weekend, wire together a full contract-driven system across REST and async messaging — and end up with more confidence in the integration than most teams get after months of manual testing.

That's the infrastructure layer AI-native software development needs.

---

## About the Author

**Kanugurajesh** — Final-year B.E. CS student at MVSR Engineering College. 4100+ GitHub contributions, hackathon winner, 2 years of internship experience at AI and SaaS companies. Building at the intersection of AI agents and reliable software systems.

- GitHub: [github.com/kanugurajesh](https://github.com/kanugurajesh)
- Email: kanugurajesh3@gmail.com
