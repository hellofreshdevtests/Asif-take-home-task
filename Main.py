# Databricks notebook source
import os
import pandas as pd
import pyspark
os.getcwd()

# COMMAND ----------

df = spark.read.option("multiline","true").format("json").load(f"file:{os.getcwd()}/input/recipes-000.json")
#https://github.com/AsifAkram134/Asif-take-home-task/blob/master/input/recipes-000.json


# COMMAND ----------

df[0]

# COMMAND ----------

df.show()

# COMMAND ----------


