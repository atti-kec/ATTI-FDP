#!/usr/bin/env python3
"""
02_train_anomaly_model.py
=========================
Trains a Spark MLlib Anomaly / Fault Detection Model on EV Charging Station Telemetry.
Saves the fitted PipelineModel to disk for real-time streaming inference.

Can be run via:
  python 02_train_anomaly_model.py
or executed interactively in JupyterLab via 02_train_anomaly_model.ipynb.
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    print("[+] Initializing PySpark Session for MLlib Model Training...")
    spark = SparkSession.builder \
        .appName("EV-Anomaly-Model-Training") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print("[+] Synthesizing 10,000 historical EV station telemetry data points...")
    # Base DataFrame with random noise
    df_raw = spark.range(0, 10000).withColumnRenamed("id", "row_id")

    # Generate realistic sensor values
    # ~90% normal records, ~10% anomalous faults
    df_sim = df_raw.withColumn("is_fault", when(rand(seed=42) < 0.10, 1).otherwise(0))

    # Normal: Temp 30-50 C, Volt 380-420 V, Current 30-100 A
    # Fault : Temp 80-110 C, Volt 280-340 V, Current 130-180 A
    df_data = df_sim.withColumn(
        "temperature_c",
        when(col("is_fault") == 1, 80.0 + rand(seed=101) * 30.0)
        .otherwise(30.0 + rand(seed=102) * 20.0)
    ).withColumn(
        "voltage_v",
        when(col("is_fault") == 1, 280.0 + rand(seed=201) * 60.0)
        .otherwise(380.0 + rand(seed=202) * 40.0)
    ).withColumn(
        "current_a",
        when(col("is_fault") == 1, 130.0 + rand(seed=301) * 50.0)
        .otherwise(30.0 + rand(seed=302) * 70.0)
    ).withColumn("label", col("is_fault").cast("double"))

    print("[+] Sample synthesized training data:")
    df_data.select("temperature_c", "voltage_v", "current_a", "label").show(5)

    # Train / Test Split
    train_df, test_df = df_data.randomSplit([0.8, 0.2], seed=42)
    print(f"[+] Training Set: {train_df.count()} rows | Test Set: {test_df.count()} rows")

    # Step 1: VectorAssembler
    # Converts multiple scalar columns into a single contiguous DenseVector in RAM
    assembler = VectorAssembler(
        inputCols=["voltage_v", "current_a", "temperature_c"],
        outputCol="raw_features"
    )

    # Step 2: StandardScaler
    # Normalizes diverse physical units (Volts, Amperes, Celsius)
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    # Step 3: Spark MLlib Classifier (Random Forest)
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=10,
        maxDepth=5,
        predictionCol="prediction"
    )

    # Build Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, rf])

    print("[+] Fitting Spark MLlib Pipeline...")
    model = pipeline.fit(train_df)

    # Evaluate
    predictions = model.transform(test_df)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"[+] Model Accuracy on Test Set: {accuracy * 100:.2f}%")

    # Save Pipeline Model
    # Check if inside docker (/workspace) or local
    output_dir = "/workspace/models/ev_anomaly_model" if os.path.exists("/workspace") else "./models/ev_anomaly_model"
    print(f"[+] Persisting trained ML model to: {output_dir}")
    
    # Overwrite if exists
    model.write().overwrite().save(output_dir)
    print("[+] Model saved successfully! Ready for real-time Structured Streaming inference.")

    spark.stop()

if __name__ == "__main__":
    main()
