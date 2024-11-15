from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os
import sys
import json
from uuid import uuid4
from pyspark.sql import Row

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('Session1').getOrCreate()

# Overview of filter or where function on Spark Dataframe
data = [("James", "Smith", "USA", "29"), ("Michael", "Rose", "USA", "22"), ("Robert", "Williams", "USA", "50")]
schema = ["Firstname", "Lastname", "Country", "Age"]
df = spark.createDataFrame(data, schema)
# df.filter("Age<50").show()
df.where(df.Country == "USA").show()

# Running SQL queries on a spark dataframe
df.createOrReplaceTempView("df_view")
spark.sql("""SELECT *
 FROM df_view 
 WHERE Age < 50""") \
 .show()