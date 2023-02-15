# Databricks notebook source
# MAGIC %md
# MAGIC Main script that runs the ETL process

# COMMAND ----------

import os
import pandas as pd
os.getcwd()

# COMMAND ----------

import pandas as pd
import json
import requests

url = 'https://raw.githubusercontent.com/AsifAkram134/Asif-take-home-task/Solution/input/recipes-000.json'
response = requests.get(url)
data = response.text.strip().split('\n')

df_list = []

for obj in data:
    try:
        parsed = json.loads(obj)
        df_list.append(parsed)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")

df = pd.DataFrame(df_list)
df


# COMMAND ----------


