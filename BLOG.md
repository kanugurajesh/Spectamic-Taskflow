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
│       └── task-events.yaml       # Kafka event contract
├── services/
│   ├── task-service/              # Flask + Kafka publisher
│   └── user-service/              # Flask user registry
├── frontend/                      # Vanilla JS Kanban board
├── specmatic.yaml                 # REST test configuration
├── specmatic-async.yaml           # Async test configuration
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
                    - taskId: 1
                      title: "Set up CI pipeline"
                      status: pending
                      priority: high

    post:
      summary: Create a task
      requestBody:
        content:
          application/json:
            examples:
              CREATE_TASK_201:
                value:
                  title: "Write API tests"
                  priority: medium
              CREATE_TASK_400:
                value:
                  priority: medium   # Missing required 'title'
      responses:
        '201':
          content:
            application/json:
              examples:
                CREATE_TASK_201:
                  value:
                    taskId: 2
                    title: "Write API tests"
                    status: pending
        '400':
          content:
            application/json:
              examples:
                CREATE_TASK_400:
                  value:
                    error: "title is required"
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
contract_tests:
  - git:
      url: "."
      branch: "main"
      specmaticConfig: specs/openapi/task-api.yaml
    baseURL: http://task-service:8080
    reportFormat: HTML

governance:
  minCoveragePercentage: 100
  maxMissedOperationsInSpec: 0
```

Two governance flags matter here:

- `minCoveragePercentage: 100` — Every single endpoint in the spec must be tested. No skipping.
- `maxMissedOperationsInSpec: 0` — If the service has endpoints not in the spec, it fails.

This makes the contract **enforcing**, not advisory.

---

## Step 3: Build the Service to Satisfy the Contract

With the failing tests as my guide, I implemented the Task Service:

```python
# services/task-service/main.py
from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
import json, os

app = Flask(__name__)
CORS(app)

tasks = {
    1: {
        "taskId": 1, "title": "Set up CI pipeline",
        "status": "pending", "priority": "high",
        "assignee": None, "createdAt": "2024-01-15T10:00:00Z"
    }
}
next_id = 2

@app.route('/tasks', methods=['GET'])
def list_tasks():
    status_filter = request.args.get('status')
    result = list(tasks.values())
    if status_filter:
        result = [t for t in result if t['status'] == status_filter]
    return jsonify(result), 200

@app.route('/tasks', methods=['POST'])
def create_task():
    data = request.get_json()
    if not data or 'title' not in data:
        return jsonify({"error": "title is required"}), 400

    global next_id
    task = {
        "taskId": next_id,
        "title": data['title'],
        "status": "pending",
        "priority": data.get('priority', 'medium'),
        "assignee": data.get('assignee'),
        "createdAt": "2024-01-15T10:00:00Z"
    }
    tasks[next_id] = task
    next_id += 1

    publish_event("task-created", {
        "taskId": task["taskId"],
        "title": task["title"],
        "status": task["status"],
        "timestamp": task["createdAt"]
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

## Step 4: Extend to Async Events with AsyncAPI

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

The async contract test works differently from REST tests. Instead of Specmatic acting as a client hitting endpoints, the `test-async` service:

1. Triggers a task creation via the REST API (`POST /tasks`)
2. Listens on the Kafka `task-created` topic
3. Validates the consumed message against the AsyncAPI schema

```yaml
# specmatic-async.yaml
contract_tests:
  - git:
      url: "."
      specmaticConfig: specs/asyncapi/task-events.yaml
    baseURL: http://task-service:8080
    kafkaBrokers: kafka:9092
```

> **Gotcha I hit:** The AsyncAPI spec initially had `host: localhost:9092`. Inside Docker's network, services communicate by service name, not localhost. Changing to `host: kafka:9092` (matching the Docker Compose service name) fixed the connection immediately. Always verify your spec's server hosts match your runtime network topology.

---

## Step 5: Mock Server for Parallel Development

While building the frontend Kanban board, the Task Service wasn't fully implemented yet. Rather than hardcoding fake data or building a separate mock, I used Specmatic's mock server:

```bash
docker compose --profile mock up mock-task-api
```

Specmatic reads `task-api.yaml`, extracts the inline examples, and serves them as HTTP responses — with zero additional configuration. `GET /tasks` returns the `TASKS_200_OK` example. `POST /tasks` with a missing title returns the `CREATE_TASK_400` example.

The frontend has a built-in API switcher in the sidebar that toggles between the mock server (`:9100`) and the real service (`:8080`). A frontend developer can work against realistic API responses before the backend exists, and switching to the real service requires changing exactly one URL.

This is the pattern that enables **parallel development without integration ceremonies**: frontend, backend, and mobile teams can all work simultaneously against the same source of truth (the spec), without coordinating implementation timelines.

---

## Step 6: Governance Enforcement in CI

The final piece is making contracts enforced — not just available.

The `docker-compose.yaml` uses `--exit-code-from` to make the entire compose stack fail if contract tests fail:

```bash
# In a CI pipeline
docker compose up --exit-code-from test-task-api
echo "Exit code: $?"
```

With `minCoveragePercentage: 100`, a developer can't ship a partial implementation. If the spec defines `DELETE /tasks/{taskId}` and the service doesn't implement it, the entire CI run fails.

Specmatic generates HTML reports at `build/reports/specmatic/test/html/index.html`. These show exactly which scenarios passed, which failed, and the diff between expected and actual responses — the artifact that goes into pull request reviews.

The governance model I enforced:

| Rule | Effect |
|------|--------|
| Every operation in the spec = tested | Can't skip endpoints |
| Every response = validated against schema | Field drift caught immediately |
| Any drift between spec and implementation | Build failure |

This turns the OpenAPI spec from passive documentation into an active quality gate.

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
│  ┌───────────────────┐   ┌──────────────────────────┐   │
│  │  test-task-api    │   │  test-async              │   │
│  │  Specmatic REST   │   │  Specmatic AsyncAPI      │   │
│  │  (OpenAPI 3.0.1)  │   │  (Kafka topic tests)     │   │
│  └───────────────────┘   └──────────────────────────┘   │
│                                                          │
│  ┌──────────────┐   ┌─────────────────────────────────┐ │
│  │  frontend    │   │  mock-task-api                  │ │
│  │  Kanban:3000 │   │  Specmatic Mock Server :9100    │ │
│  └──────────────┘   └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

---

## Contract Coverage at a Glance

| Spec File | Endpoints / Channels | Named Examples |
|-----------|---------------------|----------------|
| `specs/openapi/task-api.yaml` | 5 REST endpoints | 11 (covering 200, 201, 204, 400, 404 scenarios) |
| `specs/openapi/user-api.yaml` | 3 REST endpoints | User CRUD scenarios |
| `specs/asyncapi/task-events.yaml` | 2 Kafka channels | `task-created`, `task-updated` |

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
def publish_event(topic, payload):
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BROKERS', 'kafka:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send(topic, payload)
        producer.flush()
    except Exception as e:
        print(f"Warning: Could not publish to Kafka: {e}")
```

The service stays healthy and responds to REST calls even if Kafka is temporarily unavailable.

### 4. AsyncAPI server host resolution

The AsyncAPI spec's `servers[].host` must match the hostname that the Specmatic container can resolve. Inside Docker Compose, that's the service name (`kafka`), not `localhost`. Always check your spec's server host against your runtime network topology.

---

## Running It Yourself

```bash
git clone https://github.com/kanugurajesh/specmatic-taskflow
cd specmatic-taskflow

# Full stack + REST contract tests
docker compose up

# With async event tests
docker compose --profile async up

# Frontend against mock server (no backend needed)
docker compose --profile mock up mock-task-api frontend

# Open Specmatic Studio (visual contract editor)
docker compose --profile studio up studio
# Visit http://localhost:9000

# CI mode: fail fast on contract violations
docker compose up --exit-code-from test-task-api
```

Test reports are generated at `build/reports/specmatic/test/html/index.html`.

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
