import random
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = (
    SparkSession.builder.appName("Day5-EV-Regression")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Generate EV Charging Session dataset to predict total energy (kwh)
random.seed(7)
rows = []
for _ in range(400):
    voltage = round(random.uniform(220.0, 245.0), 1)
    current = round(random.uniform(15.0, 32.0), 1)
    duration_min = round(random.uniform(10.0, 90.0), 1)
    temp = round(random.uniform(35.0, 75.0), 1)
    # True physical power formula (P = V * I * t) + small noise
    kwh = round((voltage * current * (duration_min / 60.0)) / 1000.0 + random.uniform(-0.2, 0.2), 2)
    rows.append((voltage, current, duration_min, temp, kwh))

df = spark.createDataFrame(rows, ["voltage", "current_amp", "duration_min", "temp_c", "kwh"])
train_df, test_df = df.randomSplit([0.8, 0.2], seed=7)

# 2. Build Regression Pipeline
assembler = VectorAssembler(
    inputCols=["voltage", "current_amp", "duration_min", "temp_c"],
    outputCol="raw_features",
)
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
lr = LinearRegression(featuresCol="features", labelCol="kwh", maxIter=20, regParam=0.01)

pipeline = Pipeline(stages=[assembler, scaler, lr])
model = pipeline.fit(train_df)
preds = model.transform(test_df)

# 3. Evaluate Regression Metrics
rmse = RegressionEvaluator(labelCol="kwh", metricName="rmse").evaluate(preds)
mae = RegressionEvaluator(labelCol="kwh", metricName="mae").evaluate(preds)
r2 = RegressionEvaluator(labelCol="kwh", metricName="r2").evaluate(preds)

print("=== Linear Regression Evaluation (Predicting kWh) ===")
print(f"RMSE (Root Mean Squared Error): {rmse:.4f} kWh")
print(f"MAE  (Mean Absolute Error)    : {mae:.4f} kWh")
print(f"R2   (R-Squared Fit Score)    : {r2:.4f} (closer to 1.0 is better)")

print("\n=== Actual vs Predicted kWh ===")
preds.select("voltage", "current_amp", "duration_min", "kwh", "prediction").show(8)

spark.stop()
