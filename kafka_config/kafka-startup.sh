#!/bin/bash
set -e

echo "Starting Kafka in background..."
/etc/kafka/docker/run &
KAFKA_PID=$!

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
for i in {1..60}; do
  if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9093 > /dev/null 2>&1; then
    echo "Kafka is ready!"
    break
  fi
  if [ $i -eq 60 ]; then
    echo "Kafka failed to start in time"
    exit 1
  fi
  sleep 2
done

echo "Creating SCRAM credentials..."

# Create SCRAM credentials for test user
/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9093 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=testpassword]' \
  --entity-type users \
  --entity-name testuser

echo "================================"
echo "SCRAM credentials created successfully!"
echo "Username: testuser"
echo "Password: testpassword"
echo "================================"

# Keep container running
wait $KAFKA_PID


