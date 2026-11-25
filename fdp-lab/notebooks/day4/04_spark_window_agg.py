from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, to_timestamp, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

spark = (
    SparkSession.builder.appName("Day4-EV-Window-Agg")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("station_id", StringType()),
    StructField("voltage", DoubleType()),
    StructField("current_amp", DoubleType()),
    StructField("temp_c", DoubleType()),
    StructField("kwh", DoubleType()),
    StructField("event_time", StringType()),
])

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "station-telemetry")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("ts", to_timestamp(col("event_time")))
)

agg_df = (
    parsed_df.withWatermark("ts", "10 seconds")
    .groupBy(window(col("ts"), "10 seconds"), col("station_id"))
    .agg(
        count("*").alias("events"),
        avg("temp_c").alias("avg_temp_c"),
        avg("kwh").alias("avg_kwh"),
    )
)

query = (
    agg_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/chk_ev_window_agg")
    .start()
)

print("Spark 10-second windowed aggregation running... Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping streaming query...")
    query.stop()
