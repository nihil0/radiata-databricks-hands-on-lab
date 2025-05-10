# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Medallion Architecture with Unity Catalog
# MAGIC
# MAGIC In this lab, you will work with a raw dataset stored in a Volume.
# MAGIC You will:
# MAGIC - Create Bronze table from data exports in a  **Volume**
# MAGIC - Transform and clean it into a **Silver managed table**
# MAGIC - Create two **Gold managed tables**:
# MAGIC   - Headcount per department
# MAGIC   - Headcount per manager
# MAGIC
# MAGIC You will perform these operations in **Unity Catalog** in SQL. You may also use Python or a combination of the two. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set up your schema
# MAGIC
# MAGIC You must create a new schema to hold your Silver and Gold tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Raw Data (Bronze Layer)
# MAGIC
# MAGIC The raw Delta data is stored in a Volume:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/main/default/delta_lake_demo/bronze/employees
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transform to Silver Table
# MAGIC
# MAGIC Clean the data (e.g., type casting `HIRE_DATE`, `SALARY`, and handling nulls).  
# MAGIC Then write it as a **managed Delta table** in your schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Gold Tables
# MAGIC
# MAGIC ### Gold Table 1: Total Headcount Per Department
# MAGIC
# MAGIC Schema:
# MAGIC ```sql
# MAGIC department_id INT,
# MAGIC headcount INT
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: Total Headcount Per Manager
# MAGIC
# MAGIC Schema:
# MAGIC ```sql
# MAGIC manager_id INT,
# MAGIC headcount INT
# MAGIC ```
