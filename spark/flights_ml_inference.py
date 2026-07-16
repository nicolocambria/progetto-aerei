# =============================================================================
# flights_ml_inference.py — SPARK JOB 3: INFERENZA ML in streaming.
#
# Carica la PipelineModel salvata dal training batch e fa scoring in tempo
# reale sulla telemetria: gli eventi classificati anomali con probabilità
# ≥ PROB_WARN diventano alert su flights.ml-alerts (detector="spark_ml",
# confrontabile in dashboard con gli alert rule-based "spark_rules").
#
# Due dettagli tecnici importanti (bug della v1 corretti):
#   1. la colonna "probability" è un VectorUDT: la probabilità della classe
#      positiva si estrae con vector_to_array(col)[1], NON con getItem(1);
#   2. "alert_type" non esiste nello stream: va creata come costante lit().
# =============================================================================
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, hour, lit, when, concat, format_number,
    current_timestamp, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_IN  = os.getenv("TOPIC_IN", "flights.telemetry")
TOPIC_OUT = os.getenv("TOPIC_OUT", "flights.ml-alerts")
MODEL_PATH = os.getenv("MODEL_PATH", "/data/models/flight_anomaly_clf")
PROB_WARN = float(os.getenv("PROB_WARN", "0.60"))   # soglia alert (severity 3)
PROB_CRIT = float(os.getenv("PROB_CRIT", "0.85"))   # soglia critica (severity 4)
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/chk/flights_ml_inference")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

# Schema canonico condiviso con gli altri job.
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
    .appName("flights_ml_inference")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # i log INFO di Spark sono troppo verbosi

# La STESSA pipeline del training (indexer + assembler + random forest):
# preprocessing identico → nessun training/serving skew.
model = PipelineModel.load(MODEL_PATH)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

# Parse + filtro su TUTTI i campi del feature vector + feature "hour".
# NB (bug trovato sul campo): il VectorAssembler fallisce sui null, e la
# telemetria reale è spesso incompleta (es. baro_rate o track assenti).
# Il filtro deve coprire le stesse colonne del dropna del training.
events = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .filter(col("stream") == "telemetry")
    .filter(
        col("baro_altitude_ft").isNotNull() & col("ground_speed_kt").isNotNull()
        & col("vertical_rate_fpm").isNotNull() & col("heading_deg").isNotNull()
        & col("lat").isNotNull() & col("lon").isNotNull() & col("source").isNotNull()
    )
    .withColumn("event_ts", to_timestamp(col("`@timestamp`")))
    .filter(col("event_ts").isNotNull())
    .withColumn("hour", hour(col("event_ts")))
)

# Scoring: model.transform aggiunge prediction / probability / rawPrediction.
scored = (
    model.transform(events)
    .withColumn("p_anom", vector_to_array(col("probability"))[1])   # P(classe anomala)
)

# Solo predizioni positive sopra la soglia di warning diventano alert;
# la severità è graduata dalla probabilità stimata.
ml_alerts = (
    scored
    .filter(col("prediction") == lit(1.0))
    .withColumn("alert_type", lit("FLIGHT_ANOMALY_ML"))
    .withColumn("severity", when(col("p_anom") >= lit(PROB_CRIT), lit(4)).otherwise(lit(3)))
    .withColumn("reason", concat(lit("ML anomaly p="), format_number(col("p_anom"), 3)))
    .filter(col("p_anom") >= lit(PROB_WARN))
    .select(
        col("event_ts").alias("@timestamp"),
        current_timestamp().alias("alert_generated_at"),
        lit("flight_alert").alias("event_type"),
        lit("spark_ml").alias("detector"),      # confrontabile con "spark_rules"
        col("alert_type"),
        col("reason"),
        col("severity"),
        col("icao24"),
        col("callsign"),
        col("source"),
        col("lat"), col("lon"),
        struct(
            col("baro_altitude_ft"), col("ground_speed_kt"),
            col("vertical_rate_fpm"), col("heading_deg"),
            col("squawk"), col("p_anom"), col("ingest_ts"),
        ).alias("raw"),
    )
)

out = ml_alerts.select(
    lit(None).cast("string").alias("key"),   # key nulla → distribuzione round-robin sulle 3 partizioni
    to_json(struct(*[col(c) for c in ml_alerts.columns])).alias("value"),  # tutte le colonne in un unico JSON
)

q = (
    out.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TOPIC_OUT)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)

# Blocca il driver: il job streaming resta in esecuzione finché non viene
# fermato dall'esterno (docker stop). Senza questa riga il processo uscirebbe subito.
q.awaitTermination()
