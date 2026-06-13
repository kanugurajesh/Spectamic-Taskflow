from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

users = {
    "alice": {"id": "alice", "name": "Alice Johnson", "email": "alice@example.com", "role": "developer"},
    "bob": {"id": "bob", "name": "Bob Smith", "email": "bob@example.com", "role": "qa"},
}


@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"status": "ok"})


@app.route("/users", methods=["GET"])
def list_users():
    return jsonify(list(users.values()))


@app.route("/users", methods=["POST"])
def create_user():
    data = request.get_json(silent=True) or {}
    if not data.get("id"):
        return jsonify({"error": "Validation failed", "message": "id is required"}), 400
    users[data["id"]] = data
    return jsonify(data), 201


@app.route("/users/<user_id>", methods=["GET"])
def get_user(user_id):
    user = users.get(user_id)
    if not user:
        return jsonify({"error": "Not Found", "message": f"User with id {user_id} not found"}), 404
    return jsonify(user)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)
