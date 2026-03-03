# Crear topics via script
#/bin/bash

BOOTSTRAP_SERVER="broker-1:29092"
  PARTITIONS=4
  REPLICATION_FACTOR=2

TOPICS=(
      "sensor-telemetry"
      "sales-transactions"
      "sensor-alerts"
      "sales-summary"
)

for TOPIC in "${TOPICS[@]}"; do
    echo "Creando topic: $TOPIC"
    docker exec broker-1 kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $TOPIC \
        --partitions $PARTITIONS \
        --replication-factor $REPLICATION_FACTOR \
        --config max.message.bytes=64000 \
        --config flush.messages=1
    done

echo "Topics creados:"
docker exec broker-1 kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --list

