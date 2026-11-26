import random
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

MODEL_PATH = "/tmp/ev_fault_model"

spark = (
    SparkSession.builder.appName("Day5-EV-Classification")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Generate realistic synthetic EV telemetry history (500 sessions)
random.seed(42)
rows = []
for _ in range(500):
    station = random.choice(["S101", "S102", "S103"])
    voltage = round(random.uniform(218.0, 248.0), 1)
    current = round(random.uniform(14.0, 34.0), 1)
    temp = round(random.uniform(34.0, 86.0), 1)
    kwh = round(voltage * current * 0.0004, 2)
    # Fault / overheat rule with slight real-world noise: high temp & high current
    fault = 1.0 if (temp > 68.0 and current > 22.0) or temp > 76.0 else 0.0
    rows.append((station, voltage, current, temp, kwh, fault))

df = spark.createDataFrame(
    rows, ["station_id", "voltage", "current_amp", "temp_c", "kwh", "label"]
)

# 2. Train / Test Split (80% training, 20% unseen testing)
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Training rows: {train_df.count()}, Testing rows: {test_df.count()}")

# 3. Feature Pipeline stages
indexer = StringIndexer(inputCol="station_id", outputCol="station_idx", handleInvalid="keep")
assembler = VectorAssembler(
    inputCols=["station_idx", "voltage", "current_amp", "temp_c", "kwh"],
    outputCol="raw_features",
)
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)

# Evaluators
auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
acc_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
f1_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")

# 4. Model A: Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
lr_pipeline = Pipeline(stages=[indexer, assembler, scaler, lr])
lr_model = lr_pipeline.fit(train_df)
lr_preds = lr_model.transform(test_df)

print("\n=== Logistic Regression Evaluation ===")
print(f"Accuracy: {acc_eval.evaluate(lr_preds):.4f}")
print(f"F1 Score: {f1_eval.evaluate(lr_preds):.4f}")
print(f"AUC-ROC : {auc_eval.evaluate(lr_preds):.4f}")

# 5. Model B: Random Forest Classifier
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=30, maxDepth=5, seed=42)
rf_pipeline = Pipeline(stages=[indexer, assembler, scaler, rf])
rf_model = rf_pipeline.fit(train_df)
rf_preds = rf_model.transform(test_df)

print("\n=== Random Forest Evaluation ===")
print(f"Accuracy: {acc_eval.evaluate(rf_preds):.4f}")
print(f"F1 Score: {f1_eval.evaluate(rf_preds):.4f}")
print(f"AUC-ROC : {auc_eval.evaluate(rf_preds):.4f}")

print("\n=== Sample Predictions (Random Forest) ===")
rf_preds.select("station_id", "temp_c", "current_amp", "label", "prediction", "probability").show(8, truncate=False)

# 6. Save the trained PipelineModel for Day 4+5 Live Kafka Scoring!
rf_model.write().overwrite().save(MODEL_PATH)
print(f"Saved trained Random Forest PipelineModel to {MODEL_PATH}")

spark.stop()
