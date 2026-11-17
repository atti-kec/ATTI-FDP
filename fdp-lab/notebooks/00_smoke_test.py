# Open in Jupyter (http://localhost:8888) and run block by block. Each block checks one service.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke").getOrCreate()
print("Spark OK", spark.version)

# HDFS (needs --profile hadoop)
spark.range(5).write.mode("overwrite").parquet("hdfs://namenode:8020/tmp/smoke")
print("HDFS OK", spark.read.parquet("hdfs://namenode:8020/tmp/smoke").count())

# MySQL via JDBC (needs --profile hadoop)
df = spark.read.jdbc("jdbc:mysql://mysql:3306/ev", "subscriptions",
                     properties={"user": "root", "password": "root",
                                 "driver": "com.mysql.cj.jdbc.Driver"})
df.show()

# Kafka (needs --profile streaming)
from kafka import KafkaProducer
KafkaProducer(bootstrap_servers="kafka:29092").send("station-telemetry", b"hello").get(timeout=10)
print("Kafka OK")

# Elasticsearch (needs --profile all)
from elasticsearch import Elasticsearch
print("ES OK", Elasticsearch("http://elasticsearch:9200").info()["version"]["number"])
