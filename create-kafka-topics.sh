#!/bin/sh
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --topic task-created --partitions 3 --replication-factor 1 || true
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --topic task-updated --partitions 3 --replication-factor 1 || true
echo "Kafka topics ready: task-created, task-updated"
