import json
import os
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

tasks = {
    1: {
        "id": 1,
        "title": "Implement Login Feature",
        "description": "Create JWT-based authentication",
        "status": "in-progress",
        "assignee": "alice",
        "priority": "high",
        "createdAt": "2024-01-15T10:00:00Z",
    }
}
_next_id = 2


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


@app.route("/tasks", methods=["GET"])
def list_tasks():
    status_filter = request.args.get("status")
    result = list(tasks.values())
    if status_filter:
        result = [t for t in result if t["status"] == status_filter]
    return jsonify(result)


@app.route("/tasks", methods=["POST"])
def create_task():
    global _next_id
    data = request.get_json(silent=True) or {}
    if not data.get("title"):
        return jsonify({"error": "Validation failed", "message": "title is required"}), 400
    if not data.get("priority"):
        return jsonify({"error": "Validation failed", "message": "priority is required"}), 400

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
def get_task(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Not Found", "message": f"Task with id {task_id} not found"}), 404
    return jsonify(task)


@app.route("/tasks/<int:task_id>", methods=["PUT"])
def update_task(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Not Found", "message": f"Task with id {task_id} not found"}), 404

    data = request.get_json(silent=True) or {}
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
def delete_task(task_id):
    if task_id not in tasks:
        return jsonify({"error": "Not Found", "message": f"Task with id {task_id} not found"}), 404
    del tasks[task_id]
    return "", 204


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
