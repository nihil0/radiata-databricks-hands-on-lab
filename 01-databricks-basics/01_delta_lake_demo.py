# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake - Key Concepts
# MAGIC In this notebook, we'll explore four foundational innovations that make Delta Lake the engine behind the Lakehouse architecture:
# MAGIC
# MAGIC 1. ACID Transactions on Cloud Storage  
# MAGIC 1. Time Travel and Data Versioning  
# MAGIC 1. Schema Enforcement and Evolution
# MAGIC
# MAGIC We'll also write a Delta table to a volume to inspect its underlying folder structure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create a Volume to Store Delta Table

# COMMAND ----------

# Enter the values for your environment here
catalog = "common"
schema = "shared"
volume_name = "shared"
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/delta_lake_demo"

def show_query_results(q: str) -> None:
  display(spark.sql(q))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ACID Transactions on Cloud Storage
# MAGIC
# MAGIC > **Note:** ACID stands for Atomicity, Consistency, Isolation, and Durability. These properties ensure reliable processing of database transactions.

# COMMAND ----------

from pyspark.sql import Row

# Start with clean data
data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
df = spark.createDataFrame(data)

# Write as Delta table
df.write.format("delta").mode("overwrite").save(volume_path)

# COMMAND ----------

q = f"select * from delta.`{volume_path}`"

show_query_results(q)

# COMMAND ----------

q = f"""
-- Insert a new row
UPDATE delta.`{volume_path}`
SET name = 'Robert'
WHERE id = 2
"""

show_query_results(q)

# COMMAND ----------

q = f"""
-- update an existing row
INSERT INTO delta.`/Volumes/common/shared/shared/delta_lake_demo` (id, name)
VALUES (3, 'Catherine')
"""

show_query_results(q)

# COMMAND ----------

q = f"""
select * from delta.`{volume_path}`
order by `id`
"""

show_query_results(q)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Time Travel and Data Versioning

# COMMAND ----------

q = f"DESCRIBE HISTORY delta.`{volume_path}`"

show_query_results(q)

# COMMAND ----------

# Try reading a specific version
q = f"select * from delta.`{volume_path}` version as of ****"

show_query_results(q)

# COMMAND ----------

# try reading a specific point in time
q = f"select * from delta.`{volume_path}` timestamp as of '2025-05-07 20:00:00'"

show_query_results(q)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema Enforcement and Evolution

# COMMAND ----------

# Try inserting invalid schema (should fail)
from pyspark.sql.utils import AnalysisException

try:
    bad_df = spark.createDataFrame([Row(id=6, bad_column="Oops!")])
    bad_df.write.format("delta").mode("append").save(volume_path)
except AnalysisException as e:
    print("Schema enforcement caught an error:", e)

# Schema Evolution: adding a column
new_schema_df = spark.createDataFrame([Row(id=6, name="Leo", country="US")])
new_schema_df.write.option("mergeSchema", "true").format("delta").mode("append").save(volume_path)

# COMMAND ----------

# Show final schema
spark.read.format("delta").load(volume_path).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Physical Layout
# MAGIC Go to **Data > Volumes > delta_lake_demo > delta_table** to explore the folder structure of the Delta table (logs, parquet files, checkpoints, etc).

# COMMAND ----------

dbutils.fs.ls("/Volumes/common/shared/shared/delta_lake_demo")
