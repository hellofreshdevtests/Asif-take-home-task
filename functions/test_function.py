# Databricks notebook source
from functions import *
import pytest
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType



# COMMAND ----------

print(os.getcwd())

# COMMAND ----------

# MAGIC %run ./Script

# COMMAND ----------

# create a Spark session for testing.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

df = sparkDF
columnName = "cookTime"
error = 0
numRows = sparkDF.count()

if numRows == 0:
    dbutils.notebook.exit("Test Failed")
    
elif columnExists(df, columnName):
    # Then report the number of rows for the specified value in that column.


    print(f"There are {numRows} rows in '{df}' where '{columnName}' equals '{numRows}'.")
    
else:
    print(f"Column '{columnName}' does not exist in table '{df}' in schema (database).")
    error = 1
    dbutils.notebook.exit("Test Failed")


# COMMAND ----------

# Does the column exist?
def test_columnExists():
    assert columnExists(df, columnName) is True

# Is there at least one row for the value in the specified column?
def test_numRowsInColumnForValue():
    assert numRowsInColumnForValue(df, columnName, numRows) > 0

# COMMAND ----------


