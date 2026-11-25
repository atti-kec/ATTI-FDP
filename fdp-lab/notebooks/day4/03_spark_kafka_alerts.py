from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

spark = (
    SparkSession.builder.appName("Day4-EV-Overheat-Alerts")
    .master("local[*]")
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

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

alerts_df = parsed_df.filter(col("temp_c") > 70.0)

query = (
    alerts_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/chk_ev_alerts")
    .start()
)

print("Spark Structured Streaming listening for temp_c > 70.0... Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping streaming query...")
    query.stop()
