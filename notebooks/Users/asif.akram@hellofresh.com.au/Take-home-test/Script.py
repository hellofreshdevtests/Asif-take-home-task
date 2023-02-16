# Databricks notebook source
# MAGIC %md
# MAGIC Main script that runs the ETL process

# COMMAND ----------

# DBTITLE 1,Import dependencies
import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
import requests


# COMMAND ----------

# DBTITLE 1,Load source data from Git 
#FETCH URL OF JSON FILES
url = ['https://raw.githubusercontent.com/AsifAkram134/Asif-take-home-task/Solution/input/recipes-000.json', 'https://raw.githubusercontent.com/AsifAkram134/Asif-take-home-task/Solution/input/recipes-001.json', 'https://raw.githubusercontent.com/AsifAkram134/Asif-take-home-task/Solution/input/recipes-002.json']

#EMPTY DATAFRAME 
df_list = []
for url in url:
    response = requests.get(url)
    #SPLIT INDIVIDUAL JSON OBJECTS WITHIN EACH JSON INPUT FILE
    data = response.text.strip().split('\n')

    for obj in data:
        try:
            parsed = json.loads(obj)
            df_list.append(parsed)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")


#PASS DATAFRAME INTO A PANDAS DATAFRAME 
df = pd.DataFrame(df_list)


# COMMAND ----------

# DBTITLE 1,Data quality checks
#HALT PROCESS IF DATAFRAME IS EMPTY
if df.empty:
    dbutils.notebook.exit("Empty Dataframe")
    
#REMOVE NULL VALUES    
df_clean = df.dropna()

# COMMAND ----------


#Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("HF_ETL") \
    .getOrCreate()
#Create PySpark DataFrame from Pandas
sparkDF=spark.createDataFrame(df) 
sparkDF.printSchema()
#sparkDF.show()
#sparkDF.count()

# COMMAND ----------

#CONVERT COOKTIME & PREPTIME FROM ISO FORMAT TO SECONDS, REFERENCE: https://stackoverflow.com/questions/67338933/how-to-convert-a-time-value-inside-a-string-from-pt-format-to-seconds
sparkDF = sparkDF.withColumn(
    'cookTime', 
    F.coalesce(F.regexp_extract('cookTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 + 
    F.coalesce(F.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 
)

sparkDF = sparkDF.withColumn(
    'prepTime', 
    F.coalesce(F.regexp_extract('prepTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 + 
    F.coalesce(F.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 
)
#sparkDF.show()

# COMMAND ----------


