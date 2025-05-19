# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Unity Catalog Dynamic Views
# MAGIC
# MAGIC This demo illustrates how to use Unity Catalog Dynamic Views to restrict access to data rows **based on the current user**.
# MAGIC
# MAGIC We'll:
# MAGIC - Create a Delta table with mock employee data
# MAGIC - Map users to departments
# MAGIC - Create a dynamic view that only shows rows from the user's department

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load raw data

# COMMAND ----------

# Replace with your actual catalog/schema/volume path
csv_path = "/Volumes/common/shared/shared/databricks_learning/raw/employees_20250601.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(csv_path)
)

df.createOrReplaceTempView("employees")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the full data
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create a Mapping Table (User → Department)

# COMMAND ----------

# A table that maps users to allowed department(s)
user_dept_map = spark.createDataFrame([
    Row(user="alice@databricks.com", department_id=10),
    Row(user="bob@databricks.com", department_id=20),
    Row(user="diana@databricks.com", department_id=30),
])

user_dept_map.write.format("delta").mode("overwrite").saveAsTable("training_schema.user_department_map")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview mapping
# MAGIC SELECT * FROM training_schema.user_department_map;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the Dynamic View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW training_schema.secure_employee_view
# MAGIC AS
# MAGIC SELECT e.*
# MAGIC FROM training_schema.secure_employees e
# MAGIC JOIN training_schema.user_department_map m
# MAGIC   ON e.department_id = m.department_id
# MAGIC WHERE m.user = current_user();
# MAGIC
# MAGIC -- This view dynamically filters data based on who is querying it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Query the View
# MAGIC
# MAGIC Log in as different users and run:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM training_schema.secure_employee_view;
# MAGIC ```
# MAGIC
# MAGIC Each user will only see rows from their assigned department.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC This is how you can create row-level secure access to a shared table using **Dynamic Views** in Unity Catalog.
