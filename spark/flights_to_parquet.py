# =============================================================================
# flights_to_parquet.py — SPARK JOB 2: data lake Parquet (percorso "cold").
#
# Persiste lo stream di telemetria in file Parquet (colonnari, compressi
# Snappy) PARTIZIONATI PER DATA (event_date=YYYY-MM-DD): il partizionamento
# abilita il "partition pruning" nelle letture batch del training ML.
#
# Con STARTING_OFFSETS=earliest (impostato nel compose) il primo avvio
# rilegge TUTTA la telemetria conservata in Kafka (retention 7 giorni):
# così lo storico per il training si accumula anche se questo job non è
# rimasto acceso in continuazione — è il vantaggio del replay di Kafka.
# =============================================================================
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_IN = os.getenv("TOPIC_IN", "flights.telemetry")
PARQUET_PATH = os.getenv("PARQUET_PATH", "/data/telemetry_parquet/flights")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/chk/flights_to_parquet")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

# Stesso schema canonico del job di alerting (contratto dati condiviso).
schema = StructType([
    StructField("@timestamp", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("stream", StringType()),
    StructField("source", StringType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("baro_altitude_ft", DoubleType()),
    StructField("geo_altitude_ft", DoubleType()),
    StructField("ground_speed_kt", DoubleType()),
    StructField("heading_deg", DoubleType()),
    StructField("vertical_rate_fpm", DoubleType()),
    StructField("squawk", StringType()),
    StructField("category", StringType()),
    StructField("on_ground", BooleanType()),
])

# SparkSession = punto d'ingresso di Spark SQL / Structured Streaming.
# Timezone forzata a UTC: tutta la pipeline (Logstash, ES, Kibana) ragiona in UTC.
spark = (
    SparkSession.builder
    .appName("flights_to_parquet")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # i log INFO di Spark sono troppo verbosi

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

# Parse + pulizia minima (posizione obbligatoria) + colonna di partizione.
events = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .filter(col("stream") == "telemetry")
    .filter(col("lat").isNotNull() & col("lon").isNotNull())
    .withColumn("event_ts", to_timestamp(col("`@timestamp`")))
    .withColumn("event_date", to_date(col("event_ts")))   # chiave di partizione
    .select(
        "event_ts", "event_date", "source", "icao24", "callsign",
        "lat", "lon", "baro_altitude_ft", "geo_altitude_ft",
        "ground_speed_kt", "heading_deg", "vertical_rate_fpm",
        "squawk", "category", "on_ground", "ingest_ts",
    )
)

# Sink Parquet in append, partizionato per data. Il checkpoint garantisce
# la ripresa esatta dopo un riavvio (nessun buco, nessun duplicato).
query = (
    events.writeStream
    .format("parquet")
    .option("path", PARQUET_PATH)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

# Blocca il driver: il job streaming resta in esecuzione finché non viene
# fermato dall'esterno (docker stop). Senza questa riga il processo uscirebbe subito.
query.awaitTermination()
