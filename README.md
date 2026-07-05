# specmatic-taskflow

**AI-Native Task Management System with Executable API Contracts**

![Contract Tests](https://github.com/kanugurajesh/Spectamic-Submission/actions/workflows/ci.yml/badge.svg)

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
| Schema resiliency testing (`all` — positive + negative) | `specmatic.yaml` → `settings.test.schemaResiliencyTests` |
| Multiple OpenAPI security schemes (Basic, Bearer, API key) + role-based authorization | `task-api.yaml`, `user-api.yaml` → `components.securitySchemes`, enforced in `services/*/main.py` |
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
│   │ Frontend (Vanilla│  Kanban board SPA :3000                 │
│   │   JS + Tailwind) │  connects to mock :9100 or real :8080   │
│   └──────────────────┘                                         │
│                                                                 │
│   Specmatic (contract enforcement layer):                       │
│   ├── test-task-api   → OpenAPI + AsyncAPI tests vs Task       │
│   │                      Service and its Kafka events, one run │
│   ├── test-user-api   → OpenAPI contract test vs User Service  │
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

The final piece was enforcing that contracts don't rot, that the service handles every valid input the spec allows, and that it correctly *rejects* every invalid input — not just the single example written per endpoint. In `specmatic.yaml`:

```yaml
specmatic:
  settings:
    test:
      schemaResiliencyTests: all
  governance:
    successCriteria:
      minCoveragePercentage: 100
      maxMissedOperationsInSpec: 0
      enforce: true
```

`schemaResiliencyTests: all` tells Specmatic to expand each example into every valid positive schema variation *and* every invalid negative variation. For example, `PUT /tasks/{taskId}` has a request body with three optional enum fields (`status`, `priority`, `assignee`). On the positive side, Specmatic generates all valid combinations — catching bugs like an enum value the service forgot to handle. On the negative side, it also sends wrong types (`assignee: true`), broken enums (`priority: "urgent"`), and nulls for every field — each expecting a `4xx` response. This is strictly more thorough than `positiveOnly`, and it's what's run in CI here: it catches services that crash or silently corrupt state on malformed input, not just services that mishandle valid-but-unusual input.

`minCoveragePercentage: 100` means a service that only partially implements its contract — even if all implemented endpoints pass — will still fail CI. The spec is the floor, not a suggestion.

---

## Quick Start

Copy your Specmatic Enterprise `license.txt` into the project root before running Docker Compose:

```
specmatic-taskflow/
├── license.txt          ← your Specmatic Enterprise license (not committed)
├── docker-compose.yaml
└── ...
```

### 1. Run everything (services + frontend + contract tests)

```bash
docker compose up
```

Starts both services, the Kanban frontend, and runs the OpenAPI *and* async Kafka contract tests — one command covers the full contract suite. Open `http://localhost:3000` to see the board.

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
# First run or after any service code change — use --build to rebuild images
docker compose up test-task-api --build
docker compose up test-user-api --build

# Subsequent runs (no code changes)
docker compose up test-task-api
docker compose up test-user-api
```

**What Specmatic validates:**
- Every declared endpoint is reachable
- Response status codes match examples
- Response bodies conform to the schema (required fields, types, enums)
- Every valid AND invalid schema variation is exercised (`schemaResiliencyTests: all`)
- 100% operation coverage is enforced via `specmatic.yaml`

### Schema Resiliency Testing

With `schemaResiliencyTests: all` in `specmatic.yaml`, Specmatic automatically expands each example into the full space of valid *and* invalid inputs defined by the schema. This is applied to both services:

```bash
docker compose up test-task-api --build
docker compose up test-user-api --build
```

For `PUT /tasks/{taskId}`, the `UpdateTaskRequest` schema has three optional enum fields. On the positive side, Specmatic generates every valid combination of `status`, `priority`, and `assignee` values — without any additional example authoring. A service that only handles the documented example but silently breaks on `priority: "low"` will be caught. On the negative side, it also sends invalid types and broken enum values (`priority: "urgent"`, `assignee: true`) and expects a `4xx` response — catching services that accept malformed input instead of rejecting it.

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
docker compose up test-task-api --build
```

`specmatic.yaml` declares both the task-api (OpenAPI) and task-events (AsyncAPI) specs under one `systemUnderTest.service` — one `specmatic test` invocation runs the REST scenarios first, then the Kafka event scenarios, in the same `test-task-api` container. There's no separate async container or config file to run.

Specmatic subscribes to the `task-created` and `task-updated` Kafka topics, then runs the `before` hooks defined in `specs/asyncapi/examples/` — each one issues a real HTTP call against the Task Service (e.g. `POST /tasks`) — and validates that the resulting published messages conform to the AsyncAPI schema.

This also runs automatically in CI (`.github/workflows/ci.yml`) as part of the same "Task API + async contract tests" step, right after Kafka and the Task Service come up.

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
├── specmatic.yaml                  ← Task API (OpenAPI) + task-events (AsyncAPI) config, one systemUnderTest.service
├── specmatic-user.yaml             ← User API contract test config (own baseUrl + securitySchemes)
├── docker-compose.yaml             ← Full orchestration
├── create-kafka-topics.sh          ← Kafka topic bootstrap
│
├── specs/
│   ├── openapi/
│   │   ├── task-api.yaml           ← Task Service contract (5 endpoints, isolated example IDs)
│   │   ├── user-api.yaml           ← User Service contract (3 endpoints, 5 examples incl. 400)
│   │   └── examples/
│   │       ├── task-api/           ← External 401/403 examples, each with its own hardcoded credential
│   │       └── user-api/           ← Same, for the User Service's single apiKeyAuth scheme
│   └── asyncapi/
│       ├── task-events.yaml        ← Task event contracts (2 channels)
│       └── examples/               ← Before-hooks that trigger publishes via the REST API
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

## Security Schemes

Following the pattern from Specmatic's [`api-security-schemes`](../api-security-schemes) lab, both services protect different operations with different OpenAPI security schemes, and both authentication *and* role-based authorization are enforced in the Flask services — not just declared in the spec.

**Task API** (`specs/openapi/task-api.yaml`) varies the scheme by HTTP method:

| Operation | Scheme | Authorization rule |
|---|---|---|
| `GET /tasks`, `GET /tasks/{taskId}` | HTTP Basic | Any valid account may read |
| `POST /tasks`, `PUT /tasks/{taskId}` | Bearer token | Role must be `developer`, `manager`, or `admin` (not `qa`) |
| `DELETE /tasks/{taskId}` | API key (`X-API-Key`) | Role must be `admin` |

**User API** (`specs/openapi/user-api.yaml`) uses one scheme for all operations, with an elevated-role check on the write path:

| Operation | Scheme | Authorization rule |
|---|---|---|
| `GET /users`, `GET /users/{userId}` | API key (`X-API-Key`) | Any valid account may read |
| `POST /users` | API key (`X-API-Key`) | Role must be `admin` or `manager` |

Demo accounts (shared, hardcoded identity store in both services — see `IDENTITIES` / `API_KEYS` in `services/*/main.py`):

| Username | Role | Basic password | Bearer token | API key |
|---|---|---|---|---|
| alice | developer | `password123` | `tok_alice_dev_9f1c` | `key_alice_dev_1122` |
| bob | qa | `password234` | `tok_bob_qa_2e7a` | `key_bob_qa_3344` |
| diana | manager | `password345` | `tok_diana_mgr_5b3d` | `key_diana_mgr_5566` |
| charlie | admin | `password456` | `tok_charlie_admin_8a4f` | `key_charlie_admin_7788` |

Try it against the running services:

```bash
# 401 — no credentials
curl -i http://localhost:8080/tasks

# 200 — any valid account can read
curl -u alice:password123 http://localhost:8080/tasks

# 403 — qa is authenticated but not allowed to create tasks
curl -i -X POST http://localhost:8080/tasks \
  -H "Authorization: Bearer tok_bob_qa_2e7a" \
  -H "Content-Type: application/json" -d '{"title":"x","priority":"low"}'

# 201 — developer role is allowed
curl -X POST http://localhost:8080/tasks \
  -H "Authorization: Bearer tok_alice_dev_9f1c" \
  -H "Content-Type: application/json" -d '{"title":"x","priority":"low"}'

# 403 — developer key, but DELETE requires admin
curl -i -X DELETE http://localhost:8080/tasks/3 -H "X-API-Key: key_alice_dev_1122"

# 204 — admin key deletes successfully
curl -i -X DELETE http://localhost:8080/tasks/3 -H "X-API-Key: key_charlie_admin_7788"
```

**How Specmatic tests this:** `specmatic.yaml` (Task API) and `specmatic-user.yaml` (User API) each configure a single working `charlie`/admin credential per scheme under `runOptions.openapi.specs[].spec.securitySchemes`, keyed to the spec via a matching `id` (`taskApiSpec` / `userApiSpec`) on both the `systemUnderTest.service.definitions` entry and the `runOptions` override — so the existing example-based and schema-resiliency test scenarios keep passing against an authenticated backend. This validates *authentication* — Specmatic attaches the configured credential to every generated request for a secured operation, so a wrong or missing value there causes every scenario against that operation to fail with `401`, exactly like the reference lab demonstrates. Verified live: `docker compose run test-task-api` → 143/143 REST tests passing (100% coverage) plus 2/2 async Kafka event tests in the same run, `test-user-api` → 53/53 (100% coverage).

OpenAPI's `security` keyword only models authentication (who you are), not custom authorization rules (what your role permits) — the single credential configured above only covers the *positive*-path example and schema-resiliency scenarios. The role-based authorization above is therefore real, enforced application logic (see the curl walkthrough), and it's proven twice: once live via curl, and once as genuine, counted contract-test scenarios (next section).

**How 401/403 became real, counted coverage — not just declared response shapes:** an early version of this project declared `401`/`403` on every secured operation with no examples, reasoning that a contract-test run only ever attaches *one* configured credential per scheme, so those codes could never actually be triggered — permanently "not tested," capping coverage at 58%/56%. That reasoning turned out to be an artifact of relying solely on the global `runOptions...securitySchemes` credential, not a real limitation of Specmatic. The [`api-security-schemes`](../api-security-schemes) lab's actual contract (fetched from `specmatic/labs-contracts` at test time — not visible in this repo's local checkout of that lab) proves it: its `auth_examples/` directory has dedicated external example files like `post-orders_application_json_401.json` (a hardcoded bad bearer token) and `post-orders-create-forbidden.json` (a `before` fixture that logs into Keycloak as a real, valid, wrong-role user), each carrying its **own** credential independent of the global config — so they execute as real scenarios, hit the server, and get counted as tested.

This project now does the same thing, minus the OAuth login step (our schemes are static Basic/Bearer/API-key credentials, so no `before` fixture is needed — the bad or wrong-role value can just be hardcoded directly into the example). `specs/openapi/examples/task-api/` and `specs/openapi/examples/user-api/` each hold one external example file per 401/403 case — e.g. `post-tasks-403.json` sends `Authorization: Bearer tok_bob_qa_2e7a` (bob's real qa token — valid credential, wrong role) and expects `403`; `delete-task-401.json` sends a garbage `X-API-Key` and expects `401`. They're wired in via `data.examples.directories` in `specmatic.yaml` / `specmatic-user.yaml` (the same mechanism this project already used for the async Kafka `before`-hooks). One gotcha hit while building these: for the `basicAuth`/`apiKeyAuth` operations, omitting the auth header entirely in the example doesn't produce a 401 — Specmatic still auto-attaches the globally configured *valid* credential to fill the gap. The fix is to explicitly set the header to some other (invalid) value in the example, which takes precedence over the global default.

With these 12 examples in place, both specs measure **100% coverage with every test passing** — `minCoveragePercentage: 100` in both `specmatic.yaml` and `specmatic-user.yaml`, no artificial ceiling needed.

### Intentional Failure

Following the same technique as the [`api-security-schemes`](../api-security-schemes) lab: override the *configured* credential and re-run the exact same suite to prove authentication is real, enforced behavior — not just declared response shapes.

`TASK_BEARER_TOKEN` defaults to `charlie`'s admin token (`tok_charlie_admin_8a4f`) in `specmatic.yaml`. Override it at run time with `-e` (a plain `VAR=value` prefix on `docker compose run` does **not** reach the container unless the compose file already lists that variable — `-e` forwards it explicitly):

```bash
# Baseline — the configured admin credential works
docker compose run --rm --no-deps test-task-api
# Tests run: 143, Successes: 143, Failures: 0 — 100% coverage

# Restart before every subsequent run — the DELETE test in each run above consumes seed
# task 3, so without a restart the next run's own DELETE scenario gets a 404 instead of
# 204/403/401 and the failure/coverage counts below are off by one.
docker compose restart task-service

# 401 — override with a garbage token
docker compose run --rm --no-deps -e TASK_BEARER_TOKEN=invalid_garbage_token test-task-api
# Tests run: 143, Successes: 123, Failures: 20 — 79% coverage — governance FAILS (exit 1)
# every failure: "Specification expected status 201/200 but response contained status 401"

docker compose restart task-service

# 403 — override with bob's valid qa token
docker compose run --rm --no-deps -e TASK_BEARER_TOKEN=tok_bob_qa_2e7a test-task-api
# Tests run: 143, Successes: 123, Failures: 20 — 79% coverage — governance FAILS (exit 1)
# every failure: "...but response contained status 403" (bob authenticates fine, qa role isn't permitted)

docker compose restart task-service

# Back to the working default — full suite passes again
docker compose run --rm --no-deps test-task-api
# Tests run: 143, Successes: 143, Failures: 0 — 100% coverage
```

The same 20 scenarios fail in each override, regardless of which one — the *auto-generated* positive-path examples and schema-resiliency variations (which rely on the global `TASK_BEARER_TOKEN` credential being valid) now get whatever the broken override produces, and fail because they demand one *specific* status (the 201/200 positive-path examples, plus a couple of negative 400 examples that happen to collide). `GET /tasks`/`GET /tasks/{taskId}` (basicAuth) and `DELETE /tasks/{taskId}` (apiKeyAuth) are untouched by either override — only the bearer-secured operations move. Crucially, the 8 dedicated `task-api` auth examples described above are **unaffected by either override** — they hardcode their own credential (bob's real qa token, a garbage token, alice's real developer key) directly in the example file rather than reading `TASK_BEARER_TOKEN`, so they keep passing no matter what the environment variable is set to. Since `minCoveragePercentage` is now a strict `100`, either override also fails governance outright (`docker compose run` exits `1`) — a stronger, more obviously-broken signal than the old 58%-ceiling version, where an override could drop coverage without necessarily crossing the (already-low) threshold. `task-service` needs a restart (`docker compose restart task-service`) **before every run in this sequence, including before the first override**, not just between the two overrides — every run's own DELETE scenario consumes seed task 3, so skipping a restart leaves the next run's DELETE test facing a 404 instead of its expected status, which quietly changes the failure count.

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
A single example per endpoint only proves the service handles that one case. `schemaResiliencyTests: all` expands each example into every valid *and* invalid schema variation automatically, exposing both the gaps hand-written examples never reach and the input the service should be rejecting but silently accepts instead.

**6. Test isolation is critical for stateful services.**
Schema resiliency generates more tests, which means more mutations. A DELETE test that removes the same task ID that a GET test relies on causes cascading failures. The fix is dedicated seed data per operation: task 1 for GET (read-only), task 2 for PUT (idempotent), task 3 for DELETE (disposable). Starting `_next_id` at 100 prevents POST tests from colliding with seeded IDs.

**7. AI coding agents need executable contracts more than humans do.**
Humans can read documentation and ask questions. AI agents need machine-readable, executable constraints. Specmatic contracts are exactly that.

**8. Authentication and authorization are different testing problems.**
OpenAPI `security` schemes model authentication — Specmatic can attach a configured credential to every request and prove the service rejects wrong or missing ones. They don't model per-role authorization: a single contract-test run uses one credential per scheme, so it can't assert "admin succeeds, qa gets 403" in the same run. That check has to live in the application (and be verified directly, e.g. via curl) rather than in the contract test.

---

## Challenges I Faced

### 1. Inline examples must be symmetric

Early on, Specmatic rejected my spec because the parameter example name (`GET_TASK_200`) didn't exactly match the response example name. Both the request parameter and the response body need the same example key for Specmatic to pair them into a single test scenario. Once I understood this, I named all examples consistently across parameters, request bodies, and responses.

### 2. The healthcheck timing issue

The `task-service` depends on `kafka-init` completing before it starts (because it tries to connect to Kafka on first publish). But Docker Compose's `depends_on: condition: service_completed_successfully` only waits for the init container to exit — the service itself still needs a few seconds to be ready. Getting the `healthcheck` intervals and `start_period` right took a few iterations.

### 3. AsyncAPI server host must match the Docker network name

My initial AsyncAPI spec had `host: localhost:9092`. This works on the host machine but breaks inside Docker because containers communicate via service names, not localhost. The fix was setting `host: kafka:9092` in `task-events.yaml` to match the Docker Compose service name.

### 4. Making Kafka publish non-blocking

The task service tries to connect to Kafka on the first publish. If Kafka isn't ready yet, the connection throws. Wrapping the publish in a `try/except` means the service starts and responds to healthchecks even before Kafka is fully up — the REST contract tests still pass, and the async tests run separately after Kafka is confirmed healthy. That same `try/except` also hid a real bug for a while: `kafka-python==2.0.2` doesn't work on Python 3.12 (`No module named 'kafka.vendor.six.moves'`), so every publish was silently swallowed and logged as a warning — the contract was never actually being tested until CI started running the async suite and reported 0 messages received. Switching to `kafka-python-ng` (a maintained drop-in fork) fixed it.

### 4a. AsyncAPI test mode doesn't take a `--config` flag

`specmatic test --config=specmatic-async.yaml` silently falls back to the default `specmatic.yaml` for AsyncAPI tests — the `--config` flag only exists for the OpenAPI test path. Since `specs-task-flow`'s repo root is mounted wholesale into every Specmatic container, the real `specmatic.yaml` (the REST config) was always shadowing the async one. The fix at the time: bind-mount a separate `specmatic-async.yaml` over `/usr/src/app/specmatic.yaml` for the `test-async` service only, so that container's default config lookup resolved to the async config without touching the REST config.

**Update:** since the task-api (OpenAPI) and task-events (AsyncAPI) specs describe the same system under test (task-service), they were later merged into one `specmatic.yaml` with both an `openapi:` and `asyncapi:` block under a single `systemUnderTest.service` — this sidesteps the `--config` limitation entirely (there's only ever one default config to shadow), and `test-async`/`specmatic-async.yaml` no longer exist. A single `specmatic test` run now executes the OpenAPI scenarios first, then the AsyncAPI scenarios, in one invocation.

### 4b. AsyncAPI message examples need an external fixture + a `before` hook to actually drive anything

Inline `examples` on an AsyncAPI Message object are useful for documentation, but a "send" operation needs something to actually trigger the publish — Specmatic doesn't call your REST API on its own. The working pattern is external JSON example files (`specs/asyncapi/examples/`, referenced via `data.examples.directories` in `specmatic.yaml`) with a `before` array that issues the real HTTP request (e.g. `POST /tasks`) before Specmatic listens for the resulting Kafka message.

### 5. Specmatic Enterprise requires `/actuator/health` to run tests

Specmatic Enterprise checks for a `GET /actuator/health` endpoint on the service before executing any tests. Without it, every test scenario is marked as "Skipped" and coverage shows 0% — even if the service is fully up and the spec is correct. Adding `/actuator/health` returning `{"status": "UP"}` to both Flask services resolved this. This is separate from the application's own `/health` endpoint.

### 6. Schema resiliency + stateful in-memory store = test isolation problem

Enabling `schemaResiliencyTests: positiveOnly` expanded the task-api test suite from ~10 tests to 27. With more tests running, execution order matters. Specmatic runs operations alphabetically by HTTP method within a path (DELETE → GET → PUT). So `DELETE /tasks/1` ran first, wiped task 1, and then `GET /tasks/1` and all 11 `PUT /tasks/1` schema resiliency variations returned 404 instead of 200.

The fix was to assign each destructive or mutating operation its own dedicated seed task:
- Task 1 → `GET /tasks/{taskId}` → 200 (read-only, never touched by other operations)
- Task 2 → `PUT /tasks/{taskId}` → 200 (mutated but never deleted)
- Task 3 → `DELETE /tasks/{taskId}` → 204 (disposable)

And setting `_next_id = 100` prevents the 6 POST schema resiliency tests from creating tasks with IDs 2 and 3, which would overwrite the seed data.

### 7. `securitySchemes` only take effect through `systemUnderTest`, not ad hoc CLI args

The original `test-task-api`/`test-user-api` services ran `specmatic test <spec-file> --testBaseURL=...` — a positional-file invocation that tests a spec directly, bypassing `systemUnderTest.service` entirely (confirmed by the fact that, before adding security, both services already ran successfully against a `specmatic.yaml` whose `systemUnderTest` only ever declared `task-api.yaml`, never `user-api.yaml`). Top-level `specmatic:` settings (`governance`, `settings.test.schemaResiliencyTests`, `license`) apply either way, but `runOptions.openapi.specs[].spec.securitySchemes` is nested under `systemUnderTest.service.runOptions` and is only consulted when Specmatic resolves the spec through that block. Fixed by switching both docker-compose services to the bare `specmatic test` form, relying on `systemUnderTest.service.definitions` for which spec and `runOptions.openapi.baseUrl` for the target. Since `systemUnderTest.service` is singular, the User API also needed its own `specmatic-user.yaml` — bind-mounted over `/usr/src/app/specmatic.yaml` for that one container — rather than a second entry in the shared config. (The equivalent async config was later folded directly into `specmatic.yaml` instead of staying a separate bind-mounted file — see Challenge 4a — since the task-api and task-events specs describe the same service.)

### 8. `securitySchemes` need an explicit `id` to bind to a spec — and declaring `401`/`403` without examples breaks 100% coverage (until you give them real examples)

Two more layers surfaced only once I actually ran the suite in Docker (schema validation against `specmatic-schema.json` caught structural mistakes but not these). First: `runOptions.openapi.specs[]` didn't apply at all until the spec's `definitions.specs` entry and the `runOptions` override both carried a matching `spec.id` (`taskApiSpec` / `userApiSpec`) — without it, Specmatic fell back to auto-generated (random) credentials, and every positive scenario failed with `401` while every negative "expect some 4xx" scenario passed by accident, masking the real problem. Second, after fixing that: declaring `401`/`403` on every secured operation (schema-only, no examples) dropped measured coverage to 58% (task-api) / 56% (user-api) even with every test passing, because the coverage report treats every response code declared in an operation's `responses:` map as its own gradable unit, and — with only one configured credential per scheme attached automatically — nothing ever generated a request that actually triggered those codes, so they showed up permanently as "not tested."

My first fix was to lower `minCoveragePercentage` to that measured ceiling (58/56) and move on — CI stayed green, but I'd mistaken "the global credential override can't trigger this" for "Specmatic can't test this," which isn't the same claim. A founder review of this submission caught it by pointing at the [`api-security-schemes`](../api-security-schemes) lab: its actual contract (pulled from `specmatic/labs-contracts` at test time) has an `auth_examples/` directory of external example files that each carry their **own** credential — a hardcoded bad token, or a `before` fixture that logs into Keycloak as a real, valid, wrong-role user — completely independent of the one global credential used for the positive-path scenarios. That's the actual mechanism for testing 401/403 for real: not the `security:` keyword alone, and not a lowered coverage ceiling, but per-scenario examples that bring their own auth.

I added the equivalent here — `specs/openapi/examples/task-api/` and `specs/openapi/examples/user-api/`, 12 external example files total, wired in via `data.examples.directories` (the same mechanism already used for the async Kafka `before`-hooks) — minus the OAuth login step, since our Basic/Bearer/API-key schemes are static and the bad/wrong-role value can be hardcoded directly into the example. One extra gotcha this surfaced: for `basicAuth`/`apiKeyAuth` operations, just omitting the auth header in an example doesn't produce `401` — Specmatic still auto-attaches the globally configured *valid* credential to fill the gap, so the example has to explicitly set the header to some other value to override it. With all 12 in place, both specs measure genuine **100% coverage with every test passing** (`minCoveragePercentage: 100` in both configs, no ceiling workaround). See "Intentional Failure" under Security Schemes for the full before/after walkthrough, including why the dedicated examples keep passing even when the global credential is deliberately broken.

---

## Specmatic Academy

This project was built after completing the [Specmatic Academy](https://academy.specmatic.io/) course on Spec-Driven API Development. The certificate is attached to the submission email.

The academy course directly shaped my approach here — particularly the lessons on inline examples as the bridge between spec and test, and how the mock server inverts the typical "build first" workflow into a "contract first" one.

---

## Submission

Built as part of the Specmatic Full Stack AI Engineering Intern application.

**Email:** jobs@specmatic.io  
**Attachments:** this project folder (or GitHub link) + Specmatic Academy certificate
