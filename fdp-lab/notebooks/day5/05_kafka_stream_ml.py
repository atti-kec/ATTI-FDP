import os
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier

MODEL_PATH = "/tmp/ev_fault_model"

spark = (
    SparkSession.builder.appName("Day5-Kafka-Live-ML-Scoring")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def ensure_model_exists():
    """Load saved MLlib model, or train a fresh one if missing."""
    if os.path.exists(MODEL_PATH):
        print(f"Loaded pre-trained model from {MODEL_PATH}")
        return PipelineModel.load(MODEL_PATH)

    print("No saved model found; training quick Random Forest model first...")
    random.seed(42)
    rows = []
    for _ in range(300):
        st = random.choice(["S101", "S102", "S103"])
        v = round(random.uniform(218.0, 248.0), 1)
        c = round(random.uniform(14.0, 34.0), 1)
        t = round(random.uniform(34.0, 86.0), 1)
        kwh = round(v * c * 0.0004, 2)
        lbl = 1.0 if (t > 68.0 and c > 22.0) or t > 76.0 else 0.0
        rows.append((st, v, c, t, kwh, lbl))
    train_df = spark.createDataFrame(rows, ["station_id", "voltage", "current_amp", "temp_c", "kwh", "label"])
    pipeline = Pipeline(stages=[
        StringIndexer(inputCol="station_id", outputCol="station_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=["station_idx", "voltage", "current_amp", "temp_c", "kwh"], outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
        RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=20, seed=42),
    ])
    model = pipeline.fit(train_df)
    model.write().overwrite().save(MODEL_PATH)
    return model


ml_model = ensure_model_exists()

# Read live stream from Day 4 Kafka topic
schema = StructType([
    StructField("station_id", StringType()),
    StructField("voltage", DoubleType()),
    StructField("current_amp", DoubleType()),
    StructField("temp_c", DoubleType()),
    StructField("kwh", DoubleType()),
    StructField("event_time", StringType()),
])

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "station-telemetry")
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*").na.drop()

# Score live Kafka events directly using the trained Spark MLlib Pipeline!
scored_stream = ml_model.transform(parsed_stream)

output_df = scored_stream.select(
    "station_id", "voltage", "current_amp", "temp_c", "kwh", "prediction"
)

query = (
    output_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/chk_ev_ml_scoring")
    .start()
)

print("Live Kafka + MLlib Scoring running! Run day4/01_producer_ev.py in another terminal. Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping live ML scoring stream...")
    query.stop()
