# specmatic-taskflow

**AI-Native Task Management System with Executable API Contracts**

> A technical blog post and working project submitted for the Specmatic Full Stack AI Engineering Intern application.

---

## About Me

**Kanugu Rajesh** — B.E. Computer Science, MVSR Engineering College (2022–2026)  
[Portfolio](https://kanugurajesh.vercel.app/) · [GitHub](https://github.com/kanugurajesh) · [LinkedIn](https://linkedin.com/in/kanugurajesh/) · kanugurajesh3@gmail.com

I'm a final-year CS student with hands-on experience as a Full Stack AI Intern at two companies — Antz.ai (June 2025–Sep 2025) and DigiQuanta (Oct 2024–Apr 2025). At both, I built production-grade full-stack features: React + TypeScript frontends, Python/Flask/FastAPI backends, REST API integrations, and AI systems using RAG pipelines, LangChain, and vector databases (Qdrant, Neo4j). My side projects include [Documind](https://kanugurajesh.vercel.app/) — an AI document intelligence platform — and [FinGenie](https://kanugurajesh.vercel.app/), an AI-powered personal finance assistant built with Next.js and Supabase.

I've made 4100+ GitHub contributions, won 2 global and 3 state-level hackathons, and built a website serving 5000+ users.

The problem I kept hitting at both internships: **services break each other silently**. A backend team renames a field, a frontend team changes an assumption, and nobody finds out until production. When I discovered Specmatic and the idea of executable contracts, it immediately clicked — this is the missing enforcement layer between "we wrote an OpenAPI spec" and "the spec actually means something."

---

## Problem & Motivation

In every multi-service project I've worked on, integration bugs follow the same pattern:

1. Team A writes an API and documents it (maybe in a wiki, maybe in a Swagger file nobody updates)
2. Team B builds a consumer against that documentation
3. Six weeks later, Team A changes a field name or adds a required property
4. Team B's service breaks in production because there was nothing enforcing the contract

This gets worse with AI coding agents. When you ask an LLM to generate a service implementation, it will produce *something that looks correct* — but there's no guarantee it matches what consumers actually expect. The agent has no feedback mechanism.

**The goal of this project:** build a system where the OpenAPI and AsyncAPI specs are not passive documentation but active, executable tests — so that any implementation (human or AI-generated) either passes or fails with a precise, actionable error.

---

## What This Project Demonstrates

| Specmatic Feature | Where |
|---|---|
| REST contract testing (OpenAPI 3.0.1) | `specs/openapi/task-api.yaml`, `user-api.yaml` |
| Async event contract testing (AsyncAPI 3.0.0) | `specs/asyncapi/task-events.yaml` |
| Schema resiliency testing (`positiveOnly`) | `specmatic.yaml` → `settings.test.schemaResiliencyTests` |
| Service virtualization (mock server) | `--profile mock` |
| Governance: 100% coverage enforcement | `specmatic.yaml` |
| Specmatic Studio (visual contract IDE) | `--profile studio` |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      specmatic-taskflow                         │
│                                                                 │
│   ┌──────────────────┐        ┌──────────────────┐             │
│   │   Task Service   │        │   User Service   │             │
│   │   Flask :8080    │        │   Flask :8081    │             │
│   └────────┬─────────┘        └──────────────────┘             │
│            │ publishes on create/update                         │
│            ▼                                                    │
│   ┌──────────────────┐                                         │
│   │   Apache Kafka   │  topics: task-created, task-updated     │
│   │      :9092       │                                         │
│   └──────────────────┘                                         │
│                                                                 │
│   ┌──────────────────┐                                         │
│   │ Frontend (React) │  Kanban board SPA :3000                 │
│   │   Tailwind JS    │  connects to mock :9100 or real :8080   │
│   └──────────────────┘                                         │
│                                                                 │
│   Specmatic (contract enforcement layer):                       │
│   ├── test-task-api   → OpenAPI contract test vs Task Service  │
│   ├── test-user-api   → OpenAPI contract test vs User Service  │
│   ├── test-async      → AsyncAPI contract test vs Kafka        │
│   ├── mock-task-api   → Stub server :9100 for consumers        │
│   └── studio          → Browser contract IDE :9000             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Docker and Docker Compose v2
- A valid `specmatic-license.txt` placed at the **parent directory** of this project (i.e., alongside this folder)

---

## My Approach: Contract-First, Always

The core discipline I followed throughout this project is **write the contract before writing a single line of service code**. Here's the exact workflow:

### Step 1 — Design the API contract as an OpenAPI spec

I started by writing `specs/openapi/task-api.yaml` — not the Flask service, not the Docker setup, just the contract. I defined every endpoint, every schema, every status code, and crucially, **inline examples** for each scenario. These examples are what Specmatic uses to generate test cases.

```yaml
# In task-api.yaml — the contract comes first
/tasks/{taskId}:
  get:
    parameters:
      - name: taskId
        examples:
          GET_TASK_200:
            value: 1       # Specmatic will call GET /tasks/1
          GET_TASK_404:
            value: 999     # Specmatic will call GET /tasks/999
    responses:
      '200':
        examples:
          GET_TASK_200:
            value: { id: 1, title: "...", status: "in-progress", ... }
      '404':
        examples:
          GET_TASK_404:
            value: { error: "Not Found", message: "Task with id 999 not found" }
```

Writing the spec first forced me to think about the API design before I was locked in by implementation details. What fields are required? What are valid enum values? What does a 404 body look like?

### Step 2 — Verify the contract works as a test (fail first)

Before building the service, I ran Specmatic against a stub to confirm the test scenarios were correctly generated from the examples. This is the red phase — the contract test fails because nothing is running yet.

### Step 3 — Build the service to satisfy the contract

Only then did I write `services/task-service/main.py`. The contract is the specification — I'm not making decisions about field names or response shapes anymore, the spec already decided. This is a fundamentally different mindset from "build first, document later."

### Step 4 — Add async contracts for Kafka events

Once the REST layer was solid, I extended the approach to event-driven messaging. The Task Service publishes to Kafka on every mutation, and `specs/asyncapi/task-events.yaml` defines what those messages must look like. Specmatic validates the published messages against the AsyncAPI schema the same way it validates HTTP responses.

### Step 5 — Add governance and schema resiliency

The final piece was enforcing that contracts don't rot, and that the service handles every valid input the spec allows — not just the single example written per endpoint. In `specmatic.yaml`:

```yaml
specmatic:
  settings:
    test:
      schemaResiliencyTests: positiveOnly
  governance:
    successCriteria:
      minCoveragePercentage: 100
      maxMissedOperationsInSpec: 0
      enforce: true
```

`schemaResiliencyTests: positiveOnly` tells Specmatic to expand each example into all valid positive schema variations. For example, `PUT /tasks/{taskId}` has a request body with three optional enum fields (`status`, `priority`, `assignee`). Instead of testing one hand-written example, Specmatic generates all valid combinations — 11 tests instead of 1. This catches bugs like an enum value the service forgot to handle, or an optional field combination that produces a malformed response.

`minCoveragePercentage: 100` means a service that only partially implements its contract — even if all implemented endpoints pass — will still fail CI. The spec is the floor, not a suggestion.

---

## Quick Start

Clone or copy this project next to the other labs so the shared license file is accessible:

```
labs/
├── license.txt          ← shared Specmatic license
└── specmatic-taskflow/  ← this project
```

### 1. Run everything (services + frontend + contract tests)

```bash
docker compose up
```

Starts both services, the Kanban frontend, and runs OpenAPI contract tests. Open `http://localhost:3000` to see the board.

### 1b. Open the frontend against the mock server

```bash
docker compose --profile mock up mock-task-api frontend
```

Open `http://localhost:3000` → click **Mock Server :9100** in the sidebar.

### 2. Run with exit-code for CI

```bash
docker compose up --exit-code-from test-task-api
```

### 3. Tear down

```bash
docker compose down --remove-orphans
```

---

## Running Each Feature

### REST Contract Testing

Specmatic reads `specs/openapi/task-api.yaml` and generates test scenarios from the inline examples. It then hits the live Task Service and validates that every response matches the contract.

```bash
docker compose up test-task-api
docker compose up test-user-api
```

**What Specmatic validates:**
- Every declared endpoint is reachable
- Response status codes match examples
- Response bodies conform to the schema (required fields, types, enums)
- All valid positive schema variations are exercised (`schemaResiliencyTests: positiveOnly`)
- 100% operation coverage is enforced via `specmatic.yaml`

### Schema Resiliency Testing

With `schemaResiliencyTests: positiveOnly` in `specmatic.yaml`, Specmatic automatically expands each example into the full space of valid inputs defined by the schema. This is applied to both services:

```bash
docker compose up test-task-api --build
docker compose up test-user-api --build
```

For `PUT /tasks/{taskId}`, the `UpdateTaskRequest` schema has three optional enum fields. Specmatic generates 11 test variations covering every valid combination of `status`, `priority`, and `assignee` values — without any additional example authoring. A service that only handles the documented example but silently breaks on `priority: "low"` will be caught.

### Mock Server (Consumer-Driven Development)

Unblock frontend / consumer teams before the real service is built:

```bash
docker compose --profile mock up mock-task-api
```

The mock server is now live at `http://localhost:9100`. It serves realistic responses generated from the inline examples in `task-api.yaml` — no code needed.

Try it:
```bash
curl http://localhost:9100/tasks/1
curl -X POST http://localhost:9100/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"Fix bug","priority":"high"}'
```

### Async Kafka Contract Testing

The Task Service publishes events to Kafka when tasks are created or updated. Specmatic validates those messages against `specs/asyncapi/task-events.yaml`.

```bash
docker compose --profile async up test-async
```

Specmatic subscribes to the `task-created` and `task-updated` Kafka topics, triggers task mutations via the Task Service REST API, and then validates that the published messages conform to the AsyncAPI schema.

### Specmatic Studio

Visual IDE for exploring and editing contracts in the browser:

```bash
docker compose --profile studio up studio
```

Open `http://localhost:9000` — you can browse, edit, and run tests against the OpenAPI and AsyncAPI specs interactively.

---

## Project Structure

```
specmatic-taskflow/
├── README.md                       ← you are here
├── specmatic.yaml                  ← REST contract test config (100% coverage + schema resiliency)
├── specmatic-async.yaml            ← Async event contract test config
├── docker-compose.yaml             ← Full orchestration
├── create-kafka-topics.sh          ← Kafka topic bootstrap
│
├── specs/
│   ├── openapi/
│   │   ├── task-api.yaml           ← Task Service contract (5 endpoints, isolated example IDs)
│   │   └── user-api.yaml           ← User Service contract (3 endpoints, 4 examples)
│   └── asyncapi/
│       └── task-events.yaml        ← Task event contracts (2 channels)
│
├── frontend/
│   ├── index.html                  ← Kanban board SPA (Tailwind + vanilla JS)
│   └── Dockerfile
│
└── services/
    ├── task-service/
    │   ├── main.py                 ← Flask CRUD + CORS + Kafka publishing (seeded: tasks 1–3)
    │   ├── requirements.txt
    │   └── Dockerfile
    └── user-service/
        ├── main.py                 ← Flask CRUD + CORS (seeded: alice, bob)
        ├── requirements.txt
        └── Dockerfile
```

---

## API Contracts at a Glance

### Task API (`specs/openapi/task-api.yaml`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/tasks` | List all tasks (optional `?status=` filter) |
| POST | `/tasks` | Create a task (publishes `task-created` Kafka event) |
| GET | `/tasks/{taskId}` | Get task by ID |
| PUT | `/tasks/{taskId}` | Update task status/assignee (publishes `task-updated` event) |
| DELETE | `/tasks/{taskId}` | Delete a task |

### User API (`specs/openapi/user-api.yaml`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/users` | List all users |
| POST | `/users` | Create a user |
| GET | `/users/{userId}` | Get user by ID |

### Task Events (`specs/asyncapi/task-events.yaml`)

| Topic | Event | Trigger |
|-------|-------|---------|
| `task-created` | `TaskCreated` | POST /tasks |
| `task-updated` | `TaskUpdated` | PUT /tasks/{id} |

---

## The AI-Native Angle: Contracts as Guardrails for Coding Agents

This is the core insight of the project.

When an AI coding agent (like GitHub Copilot, Cursor, or a custom LLM-based agent) generates or modifies API code, there is no guarantee the output actually matches what consumers expect. A contract-first approach solves this:

1. **The OpenAPI spec is written first** — it is the single source of truth.
2. **The AI agent generates the service implementation** targeting the spec.
3. **Specmatic runs contract tests** against the generated implementation.
4. If the implementation deviates from the spec, tests fail — the agent gets precise, machine-readable feedback.
5. The agent iterates until `specmatic test` passes.

This creates a feedback loop where the contract acts as a hard constraint, not a suggestion. The AI agent cannot "hallucinate" a different response schema — the contract enforces correctness at every iteration.

```
  [OpenAPI Spec]
       │
       │  AI agent reads spec, generates service
       ▼
  [Generated Service Code]
       │
       │  Specmatic runs contract tests
       ▼
  [Pass / Fail + precise diff]
       │
       └── Fail → agent iterates → back to [Generated Service Code]
```

The `specmatic.yaml` governance block in this project enforces 100% operation coverage — meaning every endpoint in the spec must be hit and validated. An AI-generated service cannot skip an endpoint and still pass.

---

## Key Learnings

**1. The spec is not documentation — it is a test.**
Traditional OpenAPI specs are passive. Specmatic turns them into an active test suite that runs on every build.

**2. Contracts enable parallel development.**
The User Service team can build against a Specmatic mock of the Task API before the Task Service is complete. The contract is the handshake.

**3. AsyncAPI closes the event-driven gap.**
REST contracts are well understood, but async event schemas are often left unvalidated. The `task-events.yaml` spec ensures the Kafka messages published by the Task Service are exactly what consumers expect.

**4. Governance makes contracts enforceable.**
The `minCoveragePercentage: 100` setting means a service that partially implements its contract will fail CI. This is the difference between a spec that rots and one that stays alive.

**5. Schema resiliency reveals what examples hide.**
A single example per endpoint only proves the service handles that one case. `schemaResiliencyTests: positiveOnly` expands each example into all valid schema variations automatically, exposing gaps that hand-written examples never reach.

**6. Test isolation is critical for stateful services.**
Schema resiliency generates more tests, which means more mutations. A DELETE test that removes the same task ID that a GET test relies on causes cascading failures. The fix is dedicated seed data per operation: task 1 for GET (read-only), task 2 for PUT (idempotent), task 3 for DELETE (disposable). Starting `_next_id` at 100 prevents POST tests from colliding with seeded IDs.

**7. AI coding agents need executable contracts more than humans do.**
Humans can read documentation and ask questions. AI agents need machine-readable, executable constraints. Specmatic contracts are exactly that.

---

## Challenges I Faced

### 1. Inline examples must be symmetric

Early on, Specmatic rejected my spec because the parameter example name (`GET_TASK_200`) didn't exactly match the response example name. Both the request parameter and the response body need the same example key for Specmatic to pair them into a single test scenario. Once I understood this, I named all examples consistently across parameters, request bodies, and responses.

### 2. The healthcheck timing issue

The `task-service` depends on `kafka-init` completing before it starts (because it tries to connect to Kafka on first publish). But Docker Compose's `depends_on: condition: service_completed_successfully` only waits for the init container to exit — the service itself still needs a few seconds to be ready. Getting the `healthcheck` intervals and `start_period` right took a few iterations.

### 3. AsyncAPI server host must match the Docker network name

My initial AsyncAPI spec had `host: localhost:9092`. This works on the host machine but breaks inside Docker because containers communicate via service names, not localhost. The fix was setting `host: kafka:9092` in `task-events.yaml` to match the Docker Compose service name.

### 4. Making Kafka publish non-blocking

The task service imports `kafka-python` and tries to connect on the first publish. If Kafka isn't ready yet, the connection throws. Wrapping the publish in a `try/except` means the service starts and responds to healthchecks even before Kafka is fully up — the REST contract tests still pass, and the async tests run separately after Kafka is confirmed healthy.

### 5. Specmatic Enterprise requires `/actuator/health` to run tests

Specmatic Enterprise checks for a `GET /actuator/health` endpoint on the service before executing any tests. Without it, every test scenario is marked as "Skipped" and coverage shows 0% — even if the service is fully up and the spec is correct. Adding `/actuator/health` returning `{"status": "UP"}` to both Flask services resolved this. This is separate from the application's own `/health` endpoint.

### 6. Schema resiliency + stateful in-memory store = test isolation problem

Enabling `schemaResiliencyTests: positiveOnly` expanded the task-api test suite from ~10 tests to 27. With more tests running, execution order matters. Specmatic runs operations alphabetically by HTTP method within a path (DELETE → GET → PUT). So `DELETE /tasks/1` ran first, wiped task 1, and then `GET /tasks/1` and all 11 `PUT /tasks/1` schema resiliency variations returned 404 instead of 200.

The fix was to assign each destructive or mutating operation its own dedicated seed task:
- Task 1 → `GET /tasks/{taskId}` → 200 (read-only, never touched by other operations)
- Task 2 → `PUT /tasks/{taskId}` → 200 (mutated but never deleted)
- Task 3 → `DELETE /tasks/{taskId}` → 204 (disposable)

And setting `_next_id = 100` prevents the 6 POST schema resiliency tests from creating tasks with IDs 2 and 3, which would overwrite the seed data.

---

## Specmatic Academy

This project was built after completing the [Specmatic Academy](https://academy.specmatic.io/) course on Spec-Driven API Development. The certificate is attached to the submission email.

The academy course directly shaped my approach here — particularly the lessons on inline examples as the bridge between spec and test, and how the mock server inverts the typical "build first" workflow into a "contract first" one.

---

## Submission

Built as part of the Specmatic Full Stack AI Engineering Intern application.

**Email:** jobs@specmatic.io  
**Attachments:** this project folder (or GitHub link) + Specmatic Academy certificate
