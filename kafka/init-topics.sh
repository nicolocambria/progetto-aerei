#!/usr/bin/env bash
# =============================================================================
# Inizializzazione idempotente dei topic Kafka.
# Eseguito una sola volta dal container "kafka-init" all'avvio dello stack.
# Non ci si affida all'auto-create dei topic: crearli esplicitamente rende
# visibili subito gli errori di battitura nei nomi e fissa il partizionamento.
# =============================================================================
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

# Attende che il broker sia pronto (max ~120s): `cub kafka-ready` è l'utility
# Confluent che verifica la presenza di N broker registrati.
echo "[kafka-init] waiting for kafka at $BOOTSTRAP..."
for i in $(seq 1 60); do
  if cub kafka-ready -b "$BOOTSTRAP" 1 5 >/dev/null 2>&1; then
    echo "[kafka-init] kafka ready"; break
  fi
  echo "[kafka-init] retry $i/60"; sleep 2
done

# I 5 topic della pipeline:
#   flights.raw       → eventi non classificati (fallback di Logstash)
#   flights.dlq       → dead letter queue (eventi senza posizione)
#   flights.telemetry → stream canonico normalizzato (input dei job Spark)
#   flights.alerts    → alert rule-based (output di spark-alerts)
#   flights.ml-alerts → alert model-based (output di spark-ml-inference)
topics=(
  flights.raw
  flights.dlq
  flights.telemetry
  flights.alerts
  flights.ml-alerts
)

# --if-not-exists rende lo script idempotente: rilanciarlo non è un errore.
# 3 partizioni per topic = fino a 3 consumer paralleli per consumer group.
for t in "${topics[@]}"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 1
done

echo "[kafka-init] done"
