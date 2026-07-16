# =============================================================================
# indexer/indexer.py — Bridge Kafka → Elasticsearch (idempotente).
#
# Consuma i topic di alert (flights.alerts, flights.ml-alerts) e indicizza
# ogni documento su Elasticsearch con ID DETERMINISTICO
#   doc_id = "topic-partizione-offset"
# → riprocessare gli stessi messaggi (replay, riavvio del consumer) NON crea
#   duplicati: la stessa (topic, partition, offset) sovrascrive lo stesso doc.
#
# Questo pattern sostituisce il connettore elasticsearch-hadoop da Spark,
# fragile sull'allineamento versioni Spark/Scala/ES: qui il bridge è
# disaccoppiato, minimale e riavviabile in ogni momento.
#
# Crea anche gli indici con MAPPING ESPLICITO: in particolare "location" è
# di tipo geo_point, requisito per la mappa in Kibana.
# =============================================================================
import os
import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPICS = [t.strip() for t in os.getenv("TOPICS", "flights.alerts,flights.ml-alerts").split(",")]
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX_PREFIX = os.getenv("ES_INDEX_PREFIX", "flights")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flights-indexer")
OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")


def index_for(topic: str) -> str:
    """Convenzione di naming: flights.alerts → flights-alerts,
    flights.ml-alerts → flights-ml-alerts."""
    suffix = topic.split(".", 1)[1] if "." in topic else topic
    return f"{ES_INDEX_PREFIX}-{suffix}"


# Mapping esplicito (data catalog): keyword per i categoriali (aggregazioni
# esatte), text per reason (full-text), date per i timestamp, geo_point per
# la mappa. Un mapping esplicito evita inferenze sbagliate di Elasticsearch.
MAPPING = {
    "properties": {
        "@timestamp": {"type": "date"},
        "alert_generated_at": {"type": "date"},
        "event_type": {"type": "keyword"},
        "detector": {"type": "keyword"},
        "alert_type": {"type": "keyword"},
        "reason": {"type": "text"},
        "severity": {"type": "integer"},
        "icao24": {"type": "keyword"},
        "callsign": {"type": "keyword"},
        "source": {"type": "keyword"},
        "location": {"type": "geo_point"},       # per la mappa Kibana
        "raw": {"type": "object", "enabled": True},
    }
}


def main():
    # Attesa attiva di Elasticsearch (il container può metterci un po').
    es = Elasticsearch(ES_URL)
    while True:
        try:
            es.info(); break
        except Exception:
            time.sleep(1)

    # Creazione idempotente degli indici con mapping esplicito.
    for t in TOPICS:
        idx = index_for(t)
        if not es.indices.exists(index=idx):
            es.indices.create(index=idx, mappings=MAPPING)

    # Consumer Kafka iscritto a ENTRAMBI i topic di alert.
    #  - group_id: Kafka salva gli offset per consumer group → a ogni riavvio
    #    si riprende dall'ultimo messaggio committato (nessuna ri-lettura);
    #  - auto_offset_reset: vale solo al primissimo avvio (nessun offset
    #    salvato) e decide da dove partire ("latest" = solo messaggi nuovi).
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset=OFFSET_RESET,
        enable_auto_commit=True,       # commit periodico automatico degli offset letti
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),  # bytes Kafka → dict Python
    )
    print(f"[indexer] consuming {TOPICS} -> ES {ES_URL}", flush=True)

    n = 0
    for msg in consumer:
        doc = msg.value
        # Costruisce il geo_point per la mappa, se lat/lon presenti.
        if doc.get("lat") is not None and doc.get("lon") is not None:
            doc["location"] = {"lat": doc["lat"], "lon": doc["lon"]}
        idx = index_for(msg.topic)
        # ID deterministico → idempotenza (replay senza duplicati).
        doc_id = f"{msg.topic}-{msg.partition}-{msg.offset}"
        try:
            es.index(index=idx, id=doc_id, document=doc)
            n += 1
            if n % 20 == 0:
                print(f"[indexer] indexed={n} last={doc_id}", flush=True)
        except Exception as e:
            print(f"[indexer] ES error id={doc_id}: {e}", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
