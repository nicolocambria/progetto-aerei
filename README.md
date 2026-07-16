# Pipeline di monitoraggio voli (ADS-B)

La pipeline raccoglie in tempo reale i dati dei voli sopra
l'Italia centrale, li fa passare da Kafka e Spark per generare degli alert sui
comportamenti anomali, e li mostra su una dashboard Kibana.

Gli alert sono di due tipi: quelli **basati su regole** (codici squawk di
emergenza, quota o velocità fuori scala, ingresso in una zona sorvegliata
attorno all'aeroporto di Fiumicino) e quelli **basati su un modello di machine
learning** addestrato sullo storico dei voli raccolti.

## Flusso dei dati

```
API ADS-B  →  collector  →  Logstash  →  Kafka (flights.telemetry)
                                              │
                        ┌─────────────────────┼─────────────────────┐
                        ▼                     ▼                     ▼
                 Spark: regole         Spark: modello ML      Spark: Parquet
                        │                     │              (storico per il
                        ▼                     ▼               training)
              Kafka(flights.alerts)  Kafka(flights.ml-alerts)
                        └──────────┬──────────┘
                                   ▼
                                indexer  →  Elasticsearch  →  Kibana
```

Il collector interroga una API ADS-B pubblica e normalizza ogni aereo in un
JSON con sempre gli stessi campi e le stesse unità, così la sorgente si può
cambiare senza toccare il resto. Da lì gli eventi entrano in Kafka e vengono
letti in parallelo da tre job Spark Structured Streaming: uno applica le regole
deterministiche, uno assegna a ogni aereo una probabilità di anomalia con il
modello MLlib, uno archivia lo storico in Parquet. Gli alert prodotti tornano
su Kafka, e un indexer li porta in Elasticsearch per la visualizzazione.

## Struttura

```
collector/    lettura dell'API ADS-B e normalizzazione degli eventi
logstash/     ingresso HTTP e scrittura su Kafka
kafka/        creazione dei topic
spark/        i quattro job: regole, archiviazione Parquet, training, inferenza
indexer/      bridge Kafka → Elasticsearch
k8s/          manifest Kubernetes
scripts/      utility per i test
.env          tutta la configurazione (soglie, area monitorata, sorgente dati)
```

La cartella `data/` (checkpoint di Spark, data lake Parquet, modello
addestrato) non è nel repository: si rigenera da sola ed è pesante.

## Stack tecnologico

| | |
|---|---|
| Ingestion | Python, Logstash |
| Messaggistica | Apache Kafka (+ Zookeeper) |
| Elaborazione | Apache Spark Structured Streaming (PySpark) |
| Machine learning | Spark MLlib, Random Forest |
| Storage | Parquet (data lake), Elasticsearch |
| Visualizzazione | Kibana |
| Deploy | Docker Compose, manifest Kubernetes |
