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
# MAGIC
# MAGIC **Note to trainer**: Copy the data in the `data/` directory into a volume to which all users have read access

# COMMAND ----------

VOLUME_PATH = "/Volumes/tester/training_common/data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set up your schema
# MAGIC
# MAGIC You must create a new schema to hold your Silver and Gold tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists tester.bronze_hr;
# MAGIC create schema if not exists tester.silver_hr;
# MAGIC create schema if not exists tester.gold_hr_mart;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Raw Data (Bronze Layer)
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as fn

df = (
    spark.read.format("csv")
    .options(header="true", inferSchema="true")
    .load(f"{VOLUME_PATH}/*.csv")
    .select("*", "_metadata.file_path")
)

df.write.mode("overwrite").saveAsTable("tester.bronze_hr.employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transform to Silver Table
# MAGIC
# MAGIC Clean the data (e.g., type casting `HIRE_DATE`, `SALARY`, and handling nulls).  
# MAGIC Then write it as a **managed Delta table** in your schema.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### First make it satisfy 1NF
# MAGIC For a table to be in the first normal form, it must meet the following criteria:
# MAGIC
# MAGIC - a single cell must not hold more than one value (atomicity) ✅
# MAGIC - there must be a primary key for identification ✅ (employee_id is non null and unique per employee)
# MAGIC - no duplicated rows or columns ❌
# MAGIC - each row must have same number of columns ✅
# MAGIC
# MAGIC To remove duplicates, take latest version of row inferred from date on csv filename

# COMMAND ----------

# MAGIC %sql 
# MAGIC with base as (
# MAGIC   select *, substring(split(file_path, '_')[2], 1, 8) as snapshot_date
# MAGIC   from tester.bronze_hr.employees
# MAGIC )
# MAGIC select * from base
# MAGIC qualify row_number() over (partition by employee_id order by snapshot_date desc) = 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next we check it is 2NF
# MAGIC
# MAGIC ensure that all non-key attributes (cols other than employee_id) are fully dependent on the key (2NF)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC with base as (
# MAGIC   select *, substring(split(file_path, '_')[2], 1, 8) as snapshot_date
# MAGIC   from tester.bronze_hr.employees
# MAGIC ), 
# MAGIC first_normal_form as (
# MAGIC   select * from base
# MAGIC   qualify row_number() over (partition by employee_id order by snapshot_date desc) = 1
# MAGIC ),
# MAGIC second_normal_form as(
# MAGIC   select * except (commission_pct, file_path, snapshot_date)
# MAGIC   from first_normal_form
# MAGIC   where snapshot_date = (select max(snapshot_date) from base)
# MAGIC )
# MAGIC select * from second_normal_form limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3NF
# MAGIC This table is also 3NF since non-key attributes do not depend on other non-key attributes.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table tester.silver_hr.employees as
# MAGIC with base as (
# MAGIC   select *, substring(split(file_path, '_')[2], 1, 8) as snapshot_date
# MAGIC   from tester.bronze_hr.employees
# MAGIC ), 
# MAGIC first_normal_form as (
# MAGIC   select * from base
# MAGIC   qualify row_number() over (partition by employee_id order by snapshot_date desc) = 1
# MAGIC ),
# MAGIC second_normal_form as(
# MAGIC   select * except (commission_pct, file_path, snapshot_date) from first_normal_form
# MAGIC   where snapshot_date = (select max(snapshot_date) from base)
# MAGIC )
# MAGIC select * from second_normal_form

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To solve both problems we only need one table with employee_id, manager_id and department_id
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table tester.gold_hr_mart.d_employees as
# MAGIC select employee_id, manager_id, department_id from tester.silver_hr.employees
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists tester.gold_hr_aggregates;
# MAGIC
# MAGIC create or replace table tester.gold_hr_aggregates.headcount_per_department as
# MAGIC select department_id, count(*) as headcount from tester.gold_hr_mart.d_employees
# MAGIC group by department_id;
# MAGIC
# MAGIC
# MAGIC create or replace table tester.gold_hr_aggregates.headcount_per_manager as
# MAGIC select manager_id, count(*) as headcount from tester.gold_hr_mart.d_employees
# MAGIC where manager_id not like '%-%'
# MAGIC group by manager_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from tester.gold_hr_aggregates.headcount_per_department

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from tester.gold_hr_aggregates.headcount_per_manager
