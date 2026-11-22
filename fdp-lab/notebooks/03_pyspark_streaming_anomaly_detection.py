#!/usr/bin/env python3
"""
03_pyspark_streaming_anomaly_detection.py
=========================================
Real-time PySpark Structured Streaming pipeline:
1. Ingests raw telemetry stream from Kafka topic 'station-telemetry'.
2. Deserializes JSON and converts [voltage, current, temp] into feature vectors.
3. Scores incoming batches using the pre-trained Spark MLlib PipelineModel.
4. Filters critical anomalies (predicted fault or temperature > 75 C).
5. Writes anomaly alerts to Kafka topic 'station-alerts' (consumed by Logstash -> Elasticsearch).
6. Also streams alerts directly into Elasticsearch and prints to console.
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

def process_batch_to_es(batch_df, batch_id):
    """
    ForeachBatch micro-batch processor:
    Prints detected anomalies to console and indexes directly to Elasticsearch.
    """
    count = batch_df.count()
    if count == 0:
        return

    print(f"\n[!] >>> BATCH {batch_id}: Detected {count} CRITICAL ANOMALIES! <<<")
    # Show in console/Jupyter
    batch_df.select("timestamp", "station_id", "location", "temperature_c", "voltage_v", "prediction").show(truncate=False)

    # Direct push to Elasticsearch via Python REST client
    try:
        from elasticsearch import Elasticsearch, helpers
        es_host = os.environ.get("ES_HOST", "http://elasticsearch:9200")
        es = Elasticsearch(es_host)
        
        records = batch_df.toPandas().to_dict(orient="records")
        actions = [
            {
                "_index": "station-alerts",
                "_source": {
                    "timestamp": r["timestamp"],
                    "station_id": r["station_id"],
                    "location": r["location"],
                    "voltage_v": float(r["voltage_v"]),
                    "current_a": float(r["current_a"]),
                    "temperature_c": float(r["temperature_c"]),
                    "power_kw": float(r["power_kw"]),
                    "status": r["status"],
                    "error_code": r["error_code"],
                    "anomaly_prediction": int(r["prediction"]),
                    "alert_level": "CRITICAL" if r["temperature_c"] > 80.0 else "WARNING"
                }
            }
            for r in records
        ]
        helpers.bulk(es, actions)
        print(f"[+] Successfully indexed {len(actions)} alert documents into Elasticsearch index 'station-alerts'!")
    except Exception as e:
        print(f"[-] Note: Direct ES index skipped or Logstash is handling it ({e})")

def main():
    kafka_server = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
    input_topic = "station-telemetry"
    output_topic = "station-alerts"
    if os.path.exists("/app/models/ev_anomaly_model"):
        model_path = "/app/models/ev_anomaly_model"
    elif os.path.exists("/workspace/models/ev_anomaly_model"):
        model_path = "/workspace/models/ev_anomaly_model"
    else:
        model_path = "./models/ev_anomaly_model"

    print("=" * 70)
    print(" PySpark Structured Streaming + MLlib Anomaly Detection")
    print(f" Broker: {kafka_server} | Ingest Topic: {input_topic} | Alert Topic: {output_topic}")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("EV-Streaming-Anomaly-Detection") \
        .master("local[2]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/streaming_app") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Load Pre-trained MLlib Pipeline Model
    if not os.path.exists(model_path):
        print(f"[!] Model not found at {model_path}. Please run 02_train_anomaly_model.py first!")
        return

    print(f"[+] Loading trained ML model from: {model_path}")
    model = PipelineModel.load(model_path)
    print("[+] Model loaded successfully!")

    # 2. Schema definition for JSON stream
    telemetry_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("voltage_v", DoubleType(), True),
        StructField("current_a", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("power_kw", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("error_code", StringType(), True)
    ])

    # 3. Read Stream from Kafka
    print(f"[+] Subscribing to Kafka topic: {input_topic}")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parse JSON payload
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), telemetry_schema).alias("data")) \
        .select("data.*")

    # 5. Apply ML Model Inference in RAM
    # VectorAssembler & StandardScaler execute inside CPU L1/L2 cache during micro-batch
    df_scored = model.transform(df_parsed)

    # 6. Filter Critical Anomalies
    df_anomalies = df_scored.filter(
        (col("prediction") == 1.0) | (col("temperature_c") > 75.0) | (col("status") == "FAULT")
    )

    # 7. Sink 1: Write Anomaly Alerts back to Kafka (for Logstash -> Elasticsearch)
    kafka_alerts_df = df_anomalies.select(
        col("station_id").alias("key"),
        to_json(struct(
            col("timestamp"),
            col("station_id"),
            col("location"),
            col("voltage_v"),
            col("current_a"),
            col("temperature_c"),
            col("power_kw"),
            col("status"),
            col("error_code"),
            col("prediction").alias("anomaly_score")
        )).alias("value")
    )

    query_kafka = kafka_alerts_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/checkpoints/kafka_alerts") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    # 8. Sink 2: ForeachBatch for direct Elasticsearch bulk write and console display
    query_es = df_anomalies.writeStream \
        .foreachBatch(process_batch_to_es) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    print("[+] Streaming queries active! Listening for incoming Kafka events...")
    print("[+] Press Ctrl+C in terminal or 'Interrupt Kernel' in Jupyter to stop.")

    try:
        query_kafka.awaitTermination()
        query_es.awaitTermination()
    except KeyboardInterrupt:
        print("[+] Stopping streaming queries...")
        query_kafka.stop()
        query_es.stop()
        spark.stop()

if __name__ == "__main__":
    main()
