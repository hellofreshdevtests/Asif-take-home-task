# Databricks notebook source
# MAGIC %md
# MAGIC Main script that runs the ETL process

# COMMAND ----------

# DBTITLE 1,Import dependencies
import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import avg
from pyspark.sql.functions import format_number
import pyspark.sql.functions as F
import json
import requests
import csv


# COMMAND ----------

# DBTITLE 1,Load source data from Git repository
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

# DBTITLE 1,Run unit test function
# MAGIC %run ./functions

# COMMAND ----------

# DBTITLE 1,Convert dataframe to Spark dataframe

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

# DBTITLE 1,Unit test to check if dataframe is as expected
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

# DBTITLE 1,Extract recipes with only beef in the ingredients
beef_ingredients = sparkDF.filter(col("ingredients").rlike('(beef|Beef)'))


# COMMAND ----------

beef_ingredients.count()

# COMMAND ----------

# DBTITLE 1,Spot check if new dataframe only contains beef in ingredients
beef_ingredients.filter(~col("ingredients").rlike('(beef|Beef)')).count()

if beef_ingredients.filter(~col("ingredients").rlike('(beef|Beef)')).count() != 0:
    dbutils.notebook.exit("Test Failed")


# COMMAND ----------

# DBTITLE 1,ISO cookTime and prepTime conversion
#CONVERT COOKTIME & FROM ISO FORMAT TO SECONDS, REFERENCE: https://stackoverflow.com/questions/67338933/how-to-convert-a-time-value-inside-a-string-from-pt-format-to-seconds
beef_ingredients_converted = beef_ingredients.withColumn(
    'cookTime_converted', 
    F.coalesce(F.regexp_extract('cookTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 + 
    F.coalesce(F.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 
)



# Show the new DataFrame
#beef_ingredients_converted.show()

# COMMAND ----------

#CONVERT PREPTIME & FROM ISO FORMAT TO SECONDS, REFERENCE: https://stackoverflow.com/questions/67338933/how-to-convert-a-time-value-inside-a-string-from-pt-format-to-seconds
beef_ingredients_converted = beef_ingredients_converted.withColumn(
    'prepTime_converted', 
    F.coalesce(F.regexp_extract('prepTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 + 
    F.coalesce(F.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 
)
# Show the new DataFrame
#beef_ingredients_converted.show()

# COMMAND ----------

# DBTITLE 1,Add total cook time as a column in beef dataframe
beef_ingredients_converted = beef_ingredients_converted.withColumn("total_cook_time", col("cookTime_converted") + col("prepTime_converted"))

# Show the new DataFrame
#beef_ingredients_converted.show()

# COMMAND ----------

# DBTITLE 1,Add a column in dataframe denoting difficulty level
#Difficulty variables
easy = 1800
hard = 3600
new_df = beef_ingredients_converted.withColumn("difficulty", when(beef_ingredients_converted.total_cook_time < easy, "easy").when(beef_ingredients_converted.total_cook_time > hard, "hard").otherwise("medium"))

# Show the new DataFrame
new_df.show()

# COMMAND ----------

# DBTITLE 1,Extract dataframe grouped by difficulty and average total cook time
avg = new_df.groupBy("difficulty").agg(avg("total_cook_time"))

# COMMAND ----------

avg.show()

# COMMAND ----------

# DBTITLE 1,Convert dataframe to csv and store in dbfs and local directory


# Convert DataFrame to Pandas DataFrame
pandas_df = avg.toPandas()

# Save Pandas DataFrame as CSV file in Databricks File System (DBFS)
csv_file_path = "/dbfs/FileStore/test/output_df.csv"
output_csv = pandas_df.to_csv(csv_file_path, index=False)

# Download the CSV file to local machine
dbutils.fs.cp("file:" + csv_file_path, "file:/databricks/driver/output_df.csv")



# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/test/output_df.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #URL to csv output
# MAGIC https://hf-anz-ds.cloud.databricks.com/files/test/output_df.csv?o=2322873405429936

# COMMAND ----------

#Remove files from databricks directory after git push
dbutils.fs.rm("/dbfs/FileStore/test/output_df.csv")

# COMMAND ----------


