# =============================================================================
# Immagine base per tutti i job Spark (alerts, parquet, ml-inference, train).
# Python 3.11 slim + JRE 17 headless: PySpark è installato via pip
# (requirements.txt), quindi non serve l'immagine ufficiale Spark completa.
#
# NB: non si imposta JAVA_HOME hardcoded perché il percorso dipende
# dall'architettura (java-17-openjdk-amd64 su Intel, -arm64 su Apple Silicon):
# PySpark trova comunque `java` nel PATH tramite update-alternatives.
# =============================================================================
FROM python:3.11-slim-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . ./
