# Databricks notebook source
# MAGIC %md # Train and register a Scikit learn model for model serving
# MAGIC
# MAGIC This notebook trains an ElasticNet model using the diabetes dataset from scikit learn. Databricks autologging is also used to both log metrics and to register the trained model to the Databricks Model Registry.
# MAGIC
# MAGIC After you run this notebook in its entirety, you have a registered model for model serving with Databricks Model Serving ([AWS](https://docs.databricks.com/machine-learning/model-serving/index.html)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/model-serving/index.html)| [GCP](https://docs.gcp.databricks.com/machine-learning/model-serving/index)).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import libraries

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
from sklearn import datasets

# Import mlflow
import mlflow
import mlflow.sklearn
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

# Load Diabetes datasets
diabetes = datasets.load_diabetes()
diabetes_X = diabetes.data
diabetes_y = diabetes.target

# Create pandas DataFrame for sklearn ElasticNet linear_model
diabetes_Y = np.array([diabetes_y]).transpose()
d = np.concatenate((diabetes_X, diabetes_Y), axis=1)
cols = diabetes.feature_names + ['progression']
diabetes_data = pd.DataFrame(d, columns=cols)
# Split the data into training and test sets. (0.75, 0.25) split.
train, test = train_test_split(diabetes_data)

# The predicted column is "progression" which is a quantitative measure of disease progression one year after baseline
train_x = train.drop(["progression"], axis=1)
test_x = test.drop(["progression"], axis=1)
train_y = train[["progression"]]
test_y = test[["progression"]]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log and register the model
# MAGIC
# MAGIC The following code automattically logs the trained model. By specifying `registered_model_name` in the autologging configuration, the model trained is automatically registered to Unity Catalog. 

# COMMAND ----------

# TODO: Enter the course catalog and user schema here 
# catalog = ""
# schema = ""

# COMMAND ----------

mlflow.sklearn.autolog(log_input_examples=True, registered_model_name=f'{catalog}.{schema}.ElasticNetDiabetes')
alpha = 0.05
l1_ratio = 0.05

# Run ElasticNet
lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
lr.fit(train_x, train_y)

# COMMAND ----------


