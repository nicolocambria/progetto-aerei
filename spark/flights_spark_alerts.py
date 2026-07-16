# =============================================================================
# flights_spark_alerts.py — SPARK JOB 1: alert RULE-BASED (percorso "hot").
#
# Pipeline Structured Streaming:
#   Kafka(flights.telemetry) → parse JSON → regole deterministiche →
#   → Kafka(flights.alerts)
#
# Regole (interpretabili, standard ICAO / plausibilità fisica):
#   squawk 7500 → SQUAWK_HIJACK      (severity 5, dirottamento)
#   squawk 7600 → SQUAWK_RADIO_FAIL  (severity 4, avaria radio)
#   squawk 7700 → SQUAWK_EMERGENCY   (severity 5, emergenza generale)
#   quota  > ALT_MAX_FT   → ALT_OUTLIER    (severity 4)
#   velocità > SPEED_MAX_KT → SPEED_OUTLIER (severity 4)
#   |rateo verticale| > VS_MAX_FPM → VS_OUTLIER (severity 4)
#   posizione dentro il bounding box → GEOFENCE_ENTER (severity 3)
#
# Fault tolerance: checkpointLocation salva offset Kafka e stato → al riavvio
# il job riprende esattamente da dove era rimasto (niente perdite/duplicati).
# =============================================================================
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, when, concat, format_number,
    current_timestamp, struct, to_json, abs as sql_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

# --- Configurazione via env (vedi docker-compose.yml / .env) ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_IN  = os.getenv("TOPIC_IN", "flights.telemetry")
TOPIC_OUT = os.getenv("TOPIC_OUT", "flights.alerts")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/chk/flights_alerts")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

# --- Soglie fisiche/operative (configurabili senza toccare il codice) ---
ALT_MAX_FT   = float(os.getenv("ALT_MAX_FT", "60000"))   # oltre → implausibile per aviazione civile
SPEED_MAX_KT = float(os.getenv("SPEED_MAX_KT", "700"))   # oltre → implausibile
VS_MAX_FPM   = float(os.getenv("VS_MAX_FPM", "10000"))   # rateo verticale estremo

# --- Geofence: bounding box della "zona sorvegliata" (default: area FCO) ---
GEO_LAT_MIN = float(os.getenv("GEO_LAT_MIN", "45.40"))
GEO_LAT_MAX = float(os.getenv("GEO_LAT_MAX", "45.55"))
GEO_LON_MIN = float(os.getenv("GEO_LON_MIN", "9.10"))
GEO_LON_MAX = float(os.getenv("GEO_LON_MAX", "9.30"))

# Schema esplicito dell'evento canonico prodotto da Logstash: from_json con
# schema fisso è più robusto e veloce dell'inferenza, e documenta il contratto.
schema = StructType([
    StructField("@timestamp", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("event_type", StringType()),
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
    .appName("flights_spark_alerts")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # i log INFO di Spark sono troppo verbosi

# Sorgente streaming: topic Kafka. failOnDataLoss=false → il job non muore
# se offset storici sono stati eliminati dalla retention.
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

# Il value Kafka è bytes → cast a stringa → parse JSON con lo schema canonico.
events = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .filter(col("stream") == "telemetry")
    .withColumn("event_ts", to_timestamp(col("`@timestamp`")))
)

# ---- Classificazione a regole: la PRIMA condizione vera vince (when-chain).
# Le emergenze squawk hanno priorità sugli outlier fisici e sul geofence. ----
alert_type = (
    when(col("squawk") == lit("7500"), lit("SQUAWK_HIJACK"))
    .when(col("squawk") == lit("7600"), lit("SQUAWK_RADIO_FAIL"))
    .when(col("squawk") == lit("7700"), lit("SQUAWK_EMERGENCY"))
    .when(col("baro_altitude_ft") > lit(ALT_MAX_FT), lit("ALT_OUTLIER"))
    .when(col("ground_speed_kt")  > lit(SPEED_MAX_KT), lit("SPEED_OUTLIER"))
    .when(sql_abs(col("vertical_rate_fpm")) > lit(VS_MAX_FPM), lit("VS_OUTLIER"))
    .when(
        (col("lat") >= lit(GEO_LAT_MIN)) & (col("lat") <= lit(GEO_LAT_MAX)) &
        (col("lon") >= lit(GEO_LON_MIN)) & (col("lon") <= lit(GEO_LON_MAX)),
        lit("GEOFENCE_ENTER")
    )
    .otherwise(lit(None))  # nessuna regola scattata → non è un alert
)

# Severità graduata per tipo di alert (5=CRITICAL ... 1=INFO).
severity = (
    when(col("alert_type").isin("SQUAWK_HIJACK", "SQUAWK_EMERGENCY"), lit(5))
    .when(col("alert_type") == lit("SQUAWK_RADIO_FAIL"), lit(4))
    .when(col("alert_type").isin("ALT_OUTLIER", "SPEED_OUTLIER", "VS_OUTLIER"), lit(4))
    .when(col("alert_type") == lit("GEOFENCE_ENTER"), lit(3))
    .otherwise(lit(1))
)

# Costruzione del record di alert canonico (vedi sezione 18.2 della specifica):
# i dettagli grezzi della telemetria finiscono nel sotto-oggetto "raw".
alerts = (
    events
    .withColumn("alert_type", alert_type)
    .filter(col("alert_type").isNotNull())   # tiene solo gli eventi-alert
    .withColumn("severity", severity)
    .withColumn("reason", concat(lit("Rule alert: "), col("alert_type")))
    .select(
        col("event_ts").alias("@timestamp"),
        current_timestamp().alias("alert_generated_at"),
        lit("flight_alert").alias("event_type"),
        lit("spark_rules").alias("detector"),   # confrontabile con "spark_ml"
        col("alert_type"),
        col("reason"),
        col("severity"),
        col("icao24"),
        col("callsign"),
        col("source"),
        col("lat"), col("lon"),
        struct(
            col("baro_altitude_ft"), col("geo_altitude_ft"),
            col("ground_speed_kt"), col("heading_deg"),
            col("vertical_rate_fpm"), col("squawk"),
            col("on_ground"), col("ingest_ts"),
        ).alias("raw"),
    )
)

# Sink Kafka: il value è l'intero record serializzato in JSON.
out = alerts.select(
    lit(None).cast("string").alias("key"),   # key nulla → distribuzione round-robin sulle 3 partizioni
    to_json(struct(*[col(c) for c in alerts.columns])).alias("value"),  # tutte le colonne in un unico JSON
)

query = (
    out.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TOPIC_OUT)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)

# Blocca il driver: il job streaming resta in esecuzione finché non viene
# fermato dall'esterno (docker stop). Senza questa riga il processo uscirebbe subito.
query.awaitTermination()
