import base64
import json
import os
from datetime import datetime, timezone
from functools import wraps
import jwt
from jwt import PyJWKClient
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

# POST/PUT /tasks are secured with a real Keycloak-issued OAuth2 JWT (see keycloak/taskflow-realm.json
# for the realm/users). Signature is verified against Keycloak's JWKS endpoint; role comes from the
# token's realm_access.roles claim. Audience AND issuer verification are deliberately skipped
# (verify_aud=False, verify_iss=False): Keycloak derives the `iss` claim from whichever host/port the
# token request came through, so a browser fetch (localhost:8083) and a Docker-network fetch
# (keycloak:8080) get different issuer strings for the same realm — pinning one would break the other.
# JWKS signature verification already proves the token was issued by this exact Keycloak instance's
# private key, which is the check that actually matters for a single-realm demo like this one.
KEYCLOAK_ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/taskflow")
_jwks_client = PyJWKClient(f"{KEYCLOAK_ISSUER}/protocol/openid-connect/certs")
_KNOWN_ROLES = {"developer", "qa", "manager", "admin"}

# Demo identity store — mirrors the accounts in the User Service (and the Keycloak realm's
# users for the same four accounts/roles/passwords). GET uses Basic Auth, POST/PUT use a
# Keycloak-issued OAuth2 bearer token, DELETE uses an API key.
IDENTITIES = {
    "alice": {"password": "password123", "role": "developer", "api_key": "key_alice_dev_1122"},
    "bob": {"password": "password234", "role": "qa", "api_key": "key_bob_qa_3344"},
    "diana": {"password": "password345", "role": "manager", "api_key": "key_diana_mgr_5566"},
    "charlie": {"password": "password456", "role": "admin", "api_key": "key_charlie_admin_7788"},
}
_API_KEYS = {v["api_key"]: v["role"] for v in IDENTITIES.values()}


def _unauthorized(message, basic_challenge=False):
    response = jsonify({"error": "Unauthorized", "message": message})
    if basic_challenge:
        response.headers["WWW-Authenticate"] = 'Basic realm="TaskFlow"'
    return response, 401


def _forbidden(message):
    return jsonify({"error": "Forbidden", "message": message}), 403


def require_basic_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Basic "):
            return _unauthorized("Basic authentication required", basic_challenge=True)
        try:
            decoded = base64.b64decode(auth_header[len("Basic "):]).decode("utf-8")
            username, _, password = decoded.partition(":")
        except Exception:
            return _unauthorized("Invalid Basic authentication header", basic_challenge=True)
        identity = IDENTITIES.get(username)
        if not identity or identity["password"] != password:
            return _unauthorized("Invalid username or password", basic_challenge=True)
        return f(*args, **kwargs)
    return wrapper


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
                    token,
                    signing_key.key,
                    algorithms=["RS256"],
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


def require_api_key(allowed_roles):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            role = _API_KEYS.get(request.headers.get("X-API-Key"))
            if not role:
                return _unauthorized("Valid X-API-Key header required")
            if role not in allowed_roles:
                return _forbidden(f"role '{role}' is not permitted to perform this action")
            return f(*args, **kwargs)
        return wrapper
    return decorator

tasks = {
    1: {
        "id": 1,
        "title": "Implement Login Feature",
        "description": "Create JWT-based authentication",
        "status": "in-progress",
        "assignee": "alice",
        "priority": "high",
        "createdAt": "2024-01-15T10:00:00Z",
    },
    2: {
        "id": 2,
        "title": "Write Unit Tests",
        "description": "Add test coverage for auth module",
        "status": "pending",
        "assignee": "bob",
        "priority": "medium",
        "createdAt": "2024-01-15T11:00:00Z",
    },
    3: {
        "id": 3,
        "title": "Review Pull Request",
        "description": "Review the authentication PR",
        "status": "pending",
        "assignee": "alice",
        "priority": "low",
        "createdAt": "2024-01-15T12:00:00Z",
    },
}
_next_id = 100

TASK_STATUSES = {"pending", "in-progress", "completed"}
TASK_PRIORITIES = {"low", "medium", "high"}


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validation_error(message):
    return jsonify({"error": "Validation failed", "message": message}), 400


def _not_found(message):
    return jsonify({"error": "Not Found", "message": message}), 404


@app.errorhandler(404)
def handle_not_found(_exc):
    return _not_found("The requested resource was not found")


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


@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"status": "ok"})


@app.route("/actuator/health", methods=["GET"])
def actuator_health():
    return jsonify({"status": "UP"})


@app.route("/tasks", methods=["GET"])
@require_basic_auth
def list_tasks():
    status_filter = request.args.get("status")
    if status_filter and status_filter not in TASK_STATUSES:
        return _validation_error(f"status must be one of {sorted(TASK_STATUSES)}")
    result = list(tasks.values())
    if status_filter:
        result = [t for t in result if t["status"] == status_filter]
    return jsonify(result)


@app.route("/tasks", methods=["POST"])
@require_bearer_auth({"developer", "manager", "admin"})
def create_task():
    global _next_id
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

    task = {
        "id": _next_id,
        "title": data["title"],
        "description": data.get("description", ""),
        "status": "pending",
        "assignee": data.get("assignee", ""),
        "priority": data["priority"],
        "createdAt": _now(),
    }
    tasks[_next_id] = task
    _next_id += 1

    _publish(
        "task-created",
        {
            "taskId": task["id"],
            "title": task["title"],
            "status": task["status"],
            "assignee": task["assignee"],
            "priority": task["priority"],
            "timestamp": task["createdAt"],
        },
    )
    return jsonify(task), 201


@app.route("/tasks/<int:task_id>", methods=["GET"])
@require_basic_auth
def get_task(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Not Found", "message": f"Task with id {task_id} not found"}), 404
    return jsonify(task)


@app.route("/tasks/<int:task_id>", methods=["PUT"])
@require_bearer_auth({"developer", "manager", "admin"})
def update_task(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Not Found", "message": f"Task with id {task_id} not found"}), 404

    if not request.get_data(cache=True, as_text=True).strip():
        return _validation_error("request body is required")

    data = request.get_json(silent=True)
    if data is None or not isinstance(data, dict):
        return _validation_error("request body must be a JSON object")

    if "status" in data and data["status"] not in TASK_STATUSES:
        return _validation_error(f"status must be one of {sorted(TASK_STATUSES)}")
    if "priority" in data and data["priority"] not in TASK_PRIORITIES:
        return _validation_error(f"priority must be one of {sorted(TASK_PRIORITIES)}")
    if "assignee" in data and not isinstance(data["assignee"], str):
        return _validation_error("assignee must be a string")

    old_status = task["status"]
    if "status" in data:
        task["status"] = data["status"]
    if "assignee" in data:
        task["assignee"] = data["assignee"]
    if "priority" in data:
        task["priority"] = data["priority"]

    _publish(
        "task-updated",
        {
            "taskId": task_id,
            "previousStatus": old_status,
            "newStatus": task["status"],
            "updatedBy": data.get("assignee", ""),
            "timestamp": _now(),
        },
    )
    return jsonify(task)


@app.route("/tasks/<int:task_id>", methods=["DELETE"])
@require_api_key({"admin"})
def delete_task(task_id):
    if task_id not in tasks:
        return jsonify({"error": "Not Found", "message": f"Task with id {task_id} not found"}), 404
    del tasks[task_id]
    return "", 204


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
