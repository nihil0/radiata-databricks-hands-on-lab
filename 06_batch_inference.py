# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Inference on a Model in UC
# MAGIC
# MAGIC This notebook demonstrates how to perform batch inference on a machine learning model using UC (Unified Computing). Batch inference allows you to process multiple data points simultaneously, making it efficient for large-scale predictions.
# MAGIC
# MAGIC In this tutorial, we will cover:
# MAGIC - Loading the pre-trained model
# MAGIC - Preparing the input data for batch processing
# MAGIC - Running the batch inference
# MAGIC - Analyzing the results
# MAGIC
# MAGIC Let's get started!

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")


# TODO set these variables
catalog = "dev_sandbox"
schema = "neelabhkashyap"
model = "ElasticNetDiabetes"




# COMMAND ----------

from pyspark.sql.functions import struct, col
import mlflow.pyfunc
from sklearn.datasets import load_diabetes
import pandas as pd
from sklearn.model_selection import train_test_split

model_uri = f"models:/dev_sandbox.neelabhkashyap.elasticnetdiabetes@latest"

# Load the diabetes dataset
diabetes = load_diabetes(as_frame=True)
X = diabetes.data
y = diabetes.target

# Split the dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y)

# Prepare batch input for inference
batch_input = X_test.copy()
batch_input["batch_id"] = "batch_001"
batch_input = batch_input.reset_index(drop=True)
batch_input.head()

# Convert the batch input to a Spark DataFrame
spark_df = spark.createDataFrame(batch_input)

# Apply the model using UDF
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")
scored_df = spark_df.withColumn(
    "prediction", predict_udf(struct(*[col(c) for c in X.columns]))
)

# Write the scored DataFrame to a Parquet file
scored_df.display()
