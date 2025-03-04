from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark ML Example") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Load data
#data = pd.read_csv('data.csv')
#spark_df = spark.createDataFrame(data)

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

# Feature engineering
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
assembled_data = assembler.transform(spark_df)

# Train-test split
train_data, test_data = assembled_data.randomSplit([0.8, 0.2])

# Train model
lr = LinearRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(train_data)

# Evaluate model
test_results = lr_model.evaluate(test_data)
print(f"RMSE: {test_results.rootMeanSquaredError}")
print(f"R2: {test_results.r2}")

# Stop Spark session
spark.stop()