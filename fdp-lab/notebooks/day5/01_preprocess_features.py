from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler, StandardScaler

spark = (
    SparkSession.builder.appName("Day5-Preprocess-Features")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Sample EV Charging dataset with messy values (None/null and negative outlier)
raw_data = [
    ("S101", 230.0, 18.0, 42.0, 1.5),
    ("S102", 228.0, None, 45.0, 1.8),     # Missing current_amp
    ("S103", 242.0, 30.0, None, 3.2),     # Missing temp_c
    ("S101", -999.0, 20.0, 50.0, 2.0),    # Faulty sensor reading (outlier)
    ("S102", 240.0, 29.0, 76.0, 3.4),
    ("S103", 224.0, 16.0, 39.0, 1.1),
]
columns = ["station_id", "voltage", "current_amp", "temp_c", "kwh"]
df = spark.createDataFrame(raw_data, columns)

print("=== 1. Raw Messy EV Dataset ===")
df.show()

# 2. Data Cleaning: remove impossible voltage <= 0
clean_df = df.filter(col("voltage") > 0)

# 3. Handle missing values using Imputer (replaces null with column mean)
imputer = Imputer(
    inputCols=["current_amp", "temp_c"],
    outputCols=["current_amp_clean", "temp_c_clean"],
    strategy="mean",
)

# 4. Convert categorical text (station_id) into numeric index
indexer = StringIndexer(
    inputCol="station_id",
    outputCol="station_idx",
    handleInvalid="keep",
)

# 5. Pack all numeric features into ONE Vector column (required by Spark MLlib!)
assembler = VectorAssembler(
    inputCols=["station_idx", "voltage", "current_amp_clean", "temp_c_clean", "kwh"],
    outputCol="raw_features",
)

# 6. Scale features so large numbers (voltage ~230) don't overpower small numbers (kwh ~2)
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True,
)

# 7. Chain everything into a reusable Pipeline
pipeline = Pipeline(stages=[imputer, indexer, assembler, scaler])
model = pipeline.fit(clean_df)
result_df = model.transform(clean_df)

print("=== 2. Preprocessed & Feature-Engineered DataFrame ===")
result_df.select("station_id", "station_idx", "raw_features", "features").show(truncate=False)

spark.stop()
