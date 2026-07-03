from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

users = {
    "alice": {"id": "alice", "name": "Alice Johnson", "email": "alice@example.com", "role": "developer"},
    "bob": {"id": "bob", "name": "Bob Smith", "email": "bob@example.com", "role": "qa"},
}

USER_ROLES = {"developer", "qa", "manager", "admin"}

# Demo identity store — mirrors the accounts in the Task Service. All endpoints
# require a valid API key; creating a user additionally requires an elevated role.
API_KEYS = {
    "key_alice_dev_1122": "developer",
    "key_bob_qa_3344": "qa",
    "key_diana_mgr_5566": "manager",
    "key_charlie_admin_7788": "admin",
}


def _validation_error(message):
    return jsonify({"error": "Validation failed", "message": message}), 400


def require_api_key(allowed_roles=None):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            role = API_KEYS.get(request.headers.get("X-API-Key"))
            if not role:
                return jsonify({"error": "Unauthorized", "message": "Valid X-API-Key header required"}), 401
            if allowed_roles and role not in allowed_roles:
                return jsonify({"error": "Forbidden", "message": f"role '{role}' is not permitted to perform this action"}), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator


@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"status": "ok"})


@app.route("/actuator/health", methods=["GET"])
def actuator_health():
    return jsonify({"status": "UP"})


@app.route("/users", methods=["GET"])
@require_api_key()
def list_users():
    return jsonify(list(users.values()))


@app.route("/users", methods=["POST"])
@require_api_key({"admin", "manager"})
def create_user():
    data = request.get_json(silent=True) or {}
    for field in ("id", "name", "email"):
        if not isinstance(data.get(field), str) or not data.get(field):
            return _validation_error(f"{field} is required")
    if data.get("role") not in USER_ROLES:
        return _validation_error(f"role must be one of {sorted(USER_ROLES)}")
    users[data["id"]] = data
    return jsonify(data), 201


@app.route("/users/<user_id>", methods=["GET"])
@require_api_key()
def get_user(user_id):
    user = users.get(user_id)
    if not user:
        return jsonify({"error": "Not Found", "message": f"User with id {user_id} not found"}), 404
    return jsonify(user)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)
