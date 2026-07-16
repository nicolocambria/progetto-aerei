# =============================================================================
# train_flight_mllib.py — SPARK JOB 4: TRAINING del modello ML (batch, offline).
#
# Legge lo storico dal data lake Parquet, costruisce le WEAK LABEL
# (etichette deboli derivate dalle regole fisiche, in assenza di ground-truth
# etichettata da esperti), addestra un RandomForestClassifier dentro una
# Pipeline MLlib e la salva su disco.
#
#   label = 1 (anomalo)  se squawk ∈ {7500,7600,7700}
#                        oppure baro_altitude_ft > ALT_MAX_FT
#                        oppure ground_speed_kt > SPEED_MAX_KT
#                        oppure |vertical_rate_fpm| > VS_MAX_FPM
#   label = 0 (normale)  altrimenti
#
# La Pipeline (StringIndexer + VectorAssembler + RandomForest) è SERIALIZZATA
# per intero: l'inferenza streaming ricarica la stessa pipeline → nessun
# training/serving skew (preprocessing identico in training e in produzione).
#
# Esecuzione (a job streaming SPENTI, per non sommare due JVM Spark):
#   docker compose run --rm --no-deps spark-parquet \
#     spark-submit --driver-memory 1g /app/train_flight_mllib.py
# =============================================================================
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, hour, abs as sql_abs
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

PARQUET_PATH = os.getenv("PARQUET_PATH", "/data/telemetry_parquet/flights")
MODEL_OUT = os.getenv("MODEL_OUT", "/data/models/flight_anomaly_clf")
TEST_FRACTION = float(os.getenv("TEST_FRACTION", "0.2"))
SEED = int(os.getenv("SEED", "42"))

# Stesse soglie delle regole (coerenza tra alert rule-based e weak label).
ALT_MAX_FT   = float(os.getenv("ALT_MAX_FT", "60000"))
SPEED_MAX_KT = float(os.getenv("SPEED_MAX_KT", "700"))
VS_MAX_FPM   = float(os.getenv("VS_MAX_FPM", "10000"))


def main():
    spark = (
        SparkSession.builder
        .appName("flight-mllib-train")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Lettura batch dello storico (tutte le partizioni event_date).
    df = spark.read.parquet(PARQUET_PATH)

    # Il modello richiede feature complete: si scartano le righe con null
    # e si deriva la feature temporale "hour" (ora del giorno UTC).
    df = (
        df.dropna(subset=["event_ts", "baro_altitude_ft", "ground_speed_kt",
                          "vertical_rate_fpm", "heading_deg", "lat", "lon", "source"])
          .withColumn("hour", hour(col("event_ts")))
    )

    # --- Weak labeling: le regole fisiche generano l'etichetta ---
    is_anom = (
        col("squawk").isin("7500", "7600", "7700")
        | (col("baro_altitude_ft") > lit(ALT_MAX_FT))
        | (col("ground_speed_kt") > lit(SPEED_MAX_KT))
        | (sql_abs(col("vertical_rate_fpm")) > lit(VS_MAX_FPM))
    )
    df = df.withColumn("label", when(is_anom, lit(1.0)).otherwise(lit(0.0)))

    print("Label distribution:")
    df.groupBy("label").count().orderBy("label").show(truncate=False)

    # Guardia: senza esempi di entrambe le classi il classificatore binario
    # non è addestrabile → accumulare più storico o iniettare eventi di test.
    if df.select("label").distinct().count() < 2:
        print("ERRORE: una sola classe. Accumula più storico o abbassa le soglie.")
        raise SystemExit(2)

    # --- Gestione dello sbilanciamento di classe (migliorìa rispetto alla
    # specifica, verificata sul campo): gli anomali sono <1% del dataset e
    # senza contromisure il RandomForest resta troppo "prudente" (p_anom
    # massima ~0.2 → nessun alert supererebbe mai PROB_WARN=0.6).
    # Soluzione standard: peso di classe = n_negativi / n_positivi, passato
    # al classificatore via weightCol → le due classi pesano uguale. ---
    n_pos = df.filter(col("label") == 1.0).count()
    n_neg = df.filter(col("label") == 0.0).count()
    w_pos = float(n_neg) / max(float(n_pos), 1.0)
    df = df.withColumn("weight", when(col("label") == 1.0, lit(w_pos)).otherwise(lit(1.0)))
    print(f"Class weight positivi: {w_pos:.1f} (pos={n_pos}, neg={n_neg})")

    # Split train/test riproducibile (seed fisso).
    train, test = df.randomSplit([1.0 - TEST_FRACTION, TEST_FRACTION], seed=SEED)

    # Pipeline MLlib:
    #   1. StringIndexer  : source (categorica) → indice numerico
    #      handleInvalid="keep" → sorgenti mai viste non rompono l'inferenza
    #   2. VectorAssembler: 8 feature → un unico vettore "features"
    #   3. RandomForest   : robusto, no normalizzazione, dà probabilità
    source_indexer = StringIndexer(inputCol="source", outputCol="source_idx", handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=["baro_altitude_ft", "ground_speed_kt", "vertical_rate_fpm",
                   "heading_deg", "lat", "lon", "hour", "source_idx"],
        outputCol="features",
    )
    # weightCol → classi bilanciate; maxDepth=8 (default 5) perché gli
    # outlier fisici richiedono soglie fini su singole feature.
    clf = RandomForestClassifier(labelCol="label", featuresCol="features",
                                 weightCol="weight", maxDepth=8,
                                 numTrees=100, seed=SEED)

    pipeline = Pipeline(stages=[source_indexer, assembler, clf])
    model = pipeline.fit(train)

    # Valutazione su test set: AUC-ROC (adatta anche a classi sbilanciate).
    preds = model.transform(test)
    auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(preds)
    print(f"AUC: {auc:.4f}")
    preds.select("label", "prediction", "probability").show(20, truncate=False)

    # Salvataggio dell'INTERA pipeline (preprocessing + modello).
    model.write().overwrite().save(MODEL_OUT)
    print(f"Model saved to: {MODEL_OUT}")
    spark.stop()


if __name__ == "__main__":
    main()
