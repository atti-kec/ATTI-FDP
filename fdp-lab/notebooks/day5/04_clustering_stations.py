import random
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = (
    SparkSession.builder.appName("Day5-EV-Clustering")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Generate unlabeled EV charging session profiles (3 natural usage tiers)
random.seed(99)
rows = []
for i in range(300):
    tier = i % 3
    if tier == 0:    # Light / Slow charging
        current, temp, kwh = random.uniform(10, 16), random.uniform(32, 42), random.uniform(0.8, 1.5)
    elif tier == 1:  # Normal fast charging
        current, temp, kwh = random.uniform(20, 26), random.uniform(46, 58), random.uniform(2.0, 3.0)
    else:            # Heavy / Peak stress charging
        current, temp, kwh = random.uniform(29, 36), random.uniform(66, 82), random.uniform(3.5, 5.0)
    rows.append((f"S{101 + (i % 5)}", round(current, 1), round(temp, 1), round(kwh, 2)))

df = spark.createDataFrame(rows, ["station_id", "current_amp", "temp_c", "kwh"])

# 2. Feature Pipeline + K-Means (k=3 clusters, NO label column needed!)
assembler = VectorAssembler(inputCols=["current_amp", "temp_c", "kwh"], outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
kmeans = KMeans(featuresCol="features", k=3, seed=42)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(df)
clustered_df = model.transform(df)

# 3. Evaluate Clustering using Silhouette Score (-1 to +1; closer to +1 is tighter clusters)
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
silhouette = evaluator.evaluate(clustered_df)
print(f"=== K-Means Clustering (k=3) Silhouette Score: {silhouette:.4f} ===")

print("\n=== Cluster Summary (Average stats per discovered group) ===")
clustered_df.groupBy("prediction").avg("current_amp", "temp_c", "kwh").orderBy("prediction").show()

spark.stop()
