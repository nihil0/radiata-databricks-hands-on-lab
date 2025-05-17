# Databricks notebook source
# MAGIC %md
# MAGIC # Data De-Identification Techniques
# MAGIC
# MAGIC This notebook illustrates 3 key data privacy techniques using a sample employee dataset:
# MAGIC
# MAGIC - **Pseudonymization**: Replace identifiable fields like names with reversible pseudonyms
# MAGIC - **Token Vault**: Secure mapping between real and pseudonymized values
# MAGIC - **Generalization (Binning)**: Aggregate sensitive numerical fields like salary into defined buckets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Raw Data from Volume

# COMMAND ----------

# Replace with your actual catalog/schema/volume path
csv_path = "/Volumes/common/shared/shared/databricks_learning/raw/employees_20250601.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(csv_path)
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Pseudonymization (First Name & Last Name)
# MAGIC
# MAGIC Change the salt and note that the hash values change

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, lit, col

# Define a salt value
salt = "s0m3S@ltV@lu3"

# Create pseudonyms using SHA-256 hash with salt (reversible only via token vault)
df_pseudo = (
    df
    .withColumn("first_name_pseudo", sha2(concat_ws("", lit(salt), "FIRST_NAME"), 256))
    .withColumn("last_name_pseudo", sha2(concat_ws("", lit(salt), "LAST_NAME"), 256))
    .drop("FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE_NUMBER", "JOB_ID", "SALARY")
)
display(df_pseudo)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Token Vault Simulation
# MAGIC
# MAGIC We store a mapping table (token vault) to allow authorized reversal of pseudonyms.

# COMMAND ----------

# Build token vault for names

first_name_vault = df.select(lit("FIRST_NAME").alias("column_name"), col("FIRST_NAME").alias("value")).withColumn("token", sha2(concat_ws("", lit(salt), "value"), 256))
last_name_vault = df.select(lit("LAST_NAME").alias("column_name"), col("LAST_NAME").alias("value")).withColumn("token", sha2(concat_ws("", lit(salt), "value"), 256))

token_vault = first_name_vault.union(last_name_vault)
display(token_vault)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter token vault into two separate DataFrames for clarity
first_name_vault = token_vault.filter(col("column_name") == "FIRST_NAME").alias("f")
last_name_vault = token_vault.filter(col("column_name") == "LAST_NAME").alias("l")

# Perform joins and resolve tokens
recovered = (
    df_pseudo.alias("df")
    .join(first_name_vault, col("df.first_name_pseudo") == col("f.token"), how="left")
    .join(last_name_vault, col("df.last_name_pseudo") == col("l.token"), how="left")
    .withColumnRenamed("f.value", "FIRST_NAME")
    .withColumnRenamed("l.value", "LAST_NAME")
    .selectExpr("EMPLOYEE_ID", "f.value as FIRST_NAME", "l.value as LAST_NAME")
)

recovered.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generalization (Binning Salary)

# COMMAND ----------

from pyspark.sql.functions import when

df_binned = df.withColumn(
    "salary_band",
    when(df.SALARY < 3000, "< 3000")
    .when(df.SALARY < 5000, "3000–4999")
    .when(df.SALARY < 8000, "5000–7999")
    .when(df.SALARY < 12000, "8000–11999")
    .otherwise("12000+")
).select(
    "EMPLOYEE_ID",
    "DEPARTMENT_ID",
    "salary_band"
).display()
