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

# Create DataFrame
data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
# df = spark.createDataFrame(data=data, schema = columns)

# Create Single column dataframe
ages_list = [21, 12, 18, 42, 24]
name_list = ['John', 'Emily', 'Cassandra', 'Daisy', 'Jen']
roll_no = [1, 2, 3, 4, 5]
data = list(zip(name_list, ages_list, roll_no))
schema = StructType([

    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("roll_no", IntegerType(), True)
])

user_list = [[1, "Scott"], [2, "Henry"], [3, "John"]]
df3 = spark.createDataFrame(user_list, ["user_id", "user_name"])
df0 = spark.createDataFrame(data, schema).toDF("name", "age", "roll_no")
df1 = spark.createDataFrame(ages_list, IntegerType()).toDF("ages")
df2 = spark.createDataFrame(name_list, StringType()).toDF("names")

# Convert list of lists into spark DataFrame
data = [[1, "Scott", 25], [2, "Henry", 30], [3, "John", 45]]
columns = ["user_id", "user_name", "user_age"]
df = spark.createDataFrame(data, columns)

# Convert list of tuples into spark DataFrame
data1 = [(1, "Scott", 25), (2, "Henry", 30), (3, "John", 45)]
columns = ["user_id", "user_name", "user_age"]
df4 = spark.createDataFrame(data1, columns)

# Convert list of dictionaries into spark DataFrame
data2 = [{"user_id": 1, "user_name": "Scott", "user_age": 25},
         {"user_id": 2, "user_name": "Henry", "user_age": 30},
         {"user_id": 3, "user_name": "John", "user_age": 45}]
df5 = spark.createDataFrame(data2)
user_details = data2[1]
# Create the same dataframe using Rows
df6 = spark.createDataFrame(Row(**d) for d in data2)
# Basic datatypes in PySpark
# StringType
# IntegerType
# LongType
# DoubleType
# FloatType
# DateType
# TimestampType
# BooleanType
# ArrayType
# MapType
# StructType
# StructField
# NullType
# ByteType
# ShortType
# DecimalType
# BinaryType
# CalendarIntervalType
# Specify schema for spark dataframe using string
schema1 = '''
    name STRING,
    age INT,
    city STRING
    '''

data = [("James", 24, "New York"), ("Ann", 28, "Toronto")]
df7 = spark.createDataFrame(data, schema=schema1)

# Specify schema for spark dataframe using list
schema2 = ["name", "age", "city"]
data = [("James", 24, "New York"), ("Ann", 28, "Toronto")]
df8 = spark.createDataFrame(data, schema=schema2)

# Specify schema for spark dataframe using spark Types
schema3 = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])
data = [("James", 24, "New York"), ("Ann", 28, "Toronto")]
df9 = spark.createDataFrame(data, schema=schema3)

# Create spark dataframe using pandas DataFrame
# import pandas as pd
#
# data = [("James", 24, "New York"), ("Ann", 28, "Toronto")]
# columns = ["name", "age", "city"]
# pandas_df = pd.DataFrame(data=data, columns=columns)
# df10 = spark.createDataFrame(pandas_df)

# ArrayType columns in spark DataFrames
data = [([1, 2, 3],), ([4, 5],), ([6, 7],)]
schema = StructType([
    StructField("data", ArrayType(IntegerType()), True)
])
df11 = spark.createDataFrame(data, schema=schema)

# MapType columns in spark DataFrames
data = [({"a": 1, "b": 2},), ({"x": 3, "y": 4},)]
schema = StructType([
    StructField("data", MapType(StringType(), IntegerType()), True)
])
df12 = spark.createDataFrame(data, schema=schema)

# StructType columns in spark DataFrames
data = [((1, "Scott", 25),), ((2, "Henry", 30),)]
schema = StructType([
    StructField("data", StructType([
        StructField("user_id", IntegerType(), True),
        StructField("user_name", StringType(), True),
        StructField("user_age", IntegerType(), True)
    ]), True)
])
df13 = spark.createDataFrame(data, schema=schema)

# Create a spark dataframe to select and rename columns
data = [("James", "Wan", 24, "New York"), ("Ann", "Hathaway", 28, "Toronto")]
columns = ["firstname", "lastname", "age", "city"]
df14 = spark.createDataFrame(data, schema=columns)

df15 = df14.select("firstname", "age", "city").withColumnRenamed("name", "new_name")

# Select columns on spark dataframe
data = [("James", "Wan", 24, "New York"), ("Ann", "Hathaway", 28, "Toronto")]
columns = ["firstname", "lastname", "age", "city"]
df16 = spark.createDataFrame(data, schema=columns)
df17 = df16.select("firstname", "age", "city")
df18 = df16.select("firstname", "lastname", "age", "city",
                   F.concat(F.col("firstname"), F.lit(" "), F.col("lastname")).alias("full_name"))
df19 = spark.createDataFrame([
    (2, "Alice"), (5, "Bob")], schema=["age", "name"])
df19.selectExpr("age * 2", "name")

# Refering columns using spark dataframe names
data = [("James", "Wan", 24, "New York"), ("Ann", "Hathaway", 28, "Toronto")]
columns = ["firstname", "lastname", "age", "city"]
df20 = spark.createDataFrame(data, schema=columns)
df21 = df20.select(df20.firstname, df20.age)
df22 = df20.select(df20["firstname"], df20["lastname"], df20["age"], df20["city"],
                   (df20["firstname"] + F.lit(" ") + df20["lastname"]).alias(
                       "fullname"))  # This won't work without f.concat or f.lit

# Understanding cols function
data = [("James", "Wan", 24, "New York"), ("Ann", "Hathaway", 28, "Toronto")]
columns = ["firstname", "lastname", "age", "city"]
df23 = spark.createDataFrame(data, schema=columns)
df24 = df23.select(F.col("firstname"), F.col("age"))
df25 = df23.select(F.col("firstname"), F.col("lastname"), F.col("age"), F.col("city"),
                   F.concat(F.col("firstname"), F.lit(", "), F.col("lastname")).alias(
                       "fullname"))  # try using F.concat to add two columns

from pyspark.sql.functions import col
cols = ["firstname", "lastname", "age", "city"]
df26 = df23.select(*[col(col_name) for col_name in cols])

# Invoking functions using spark column objects
data = [("James", "Wan", 24, "New York"), ("Ann", "Hathaway", 28, "Toronto")]
columns = ["firstname", "lastname", "age", "city"]
df27 = spark.createDataFrame(data, schema=columns)

# Concatenate firstname and lastname using alias fullname
df28 = df27.select(F.concat(F.col("firstname"), F.lit(" "), F.col("lastname")).alias("fullname"))   # This won't work without f.concat or f.lit
#df28.show()
# To add a column fullname to the original dataframe
df29 = df27.withColumn("fullname", F.concat(F.col("firstname"), F.lit(" "), F.col("lastname")))

# Renaming spark dataframe columns or expressions
data = [("James", "Wan", 24, "New York"), ("Ann", "Hathaway", 28, "Toronto")]
columns = ["firstname", "lastname", "age", "city"]
df30 = spark.createDataFrame(data, schema=columns)
df31 = df30.select(F.col("firstname").alias("fname"), F.col("lastname").alias("lname"))
# Rename columns using withColumnRenamed
df32 = df30.withColumnRenamed("firstname", "fname").withColumnRenamed("lastname", "lname")
# Predefined functions using spark dataframe API
# df34 = spark.createDataFrame(data, schema=columns)
# df35 = df34.select(F.concat(F.col("firstname"), F.lit(" "), F.col("lastname")).alias("fullname"))
# df36 = df34.select(F.concat_ws(" ", F.col("firstname"), F.col("lastname")).alias("fullname"))
# df37 = df34.select(F.lower(F.col("firstname")).alias("lower"))
# df38 = df34.select(F.upper(F.col("firstname")).alias("upper"))
# df39 = df34.select(F.length(F.col("firstname")).alias("length"))
# df40 = df34.select(F.trim(F.col("firstname")).alias("trim"))
# df41 = df34.select(F.ltrim(F.col("firstname")).alias("ltrim"))
# df42 = df34.select(F.rtrim(F.col("firstname")).alias("rtrim"))
# df43 = df34.select(F.reverse(F.col("firstname")).alias("reverse"))
# df44 = df34.select(F.substring(F.col("firstname"), 1, 3).alias("substring"))
# df45 = df34.select(F.initcap(F.col("firstname")).alias("initcap"))
# df46 = df34.select(F.coalesce(F.col("firstname"), F.col("lastname")).alias("coalesce"))
# df47 = df34.select(F.col("firstname").cast("int").alias("cast"))
# df48 = df34.select(F.col("firstname").between("A", "L").alias("between"))
# df49 = df34.select(F.col("firstname").contains("J").alias("contains"))
# df50 = df34.select(F.col("firstname").startswith("J").alias("startswith"))
# df51 = df34.select(F.col("firstname").endswith("s").alias("endswith"))
# df52 = df34.select(F.col("firstname").isNull().alias("isNull"))
# df53 = df34.select(F.col("firstname").isNotNull().alias("isNotNull"))
# df54 = df34.select(F.col("firstname").isin("James", "Ann").alias("isin"))
# df55 = df34.select(F.col("firstname").like("J%").alias("like"))
# df56 = df34.select(F.col("firstname").rlike("J.*").alias("rlike"))
# df57 = df34.select(F.col("firstname").substr(1, 3).alias("substr"))
# df58 = df34.select(F.col("firstname").when(F.col("firstname") == "James", "True").alias("when"))
# df59 = df34.select(F.col("firstname").otherwise("False").alias("otherwise"))
# df60 = df34.select(F.col("firstname").cast("int").alias("cast"))
# df61 = df34.select(F.col("firstname").asc().alias("asc"))
# df62 = df34.select(F.col("firstname").desc().alias("desc"))
# df63 = df34.select(F.col("firstname").desc_nulls_first().alias("desc_nulls_first"))
# df64 = df34.select(F.col("firstname").desc_nulls_last().alias("desc_nulls_last"))
# df65 = df34.select(F.col("firstname").asc_nulls_first().alias("asc_nulls_first"))
# df66 = df34.select(F.col("firstname").asc_nulls_last().alias("asc_nulls_last"))

# Extracting strings using substrings from spark dataframe columns
df67 = spark.createDataFrame(data, schema=columns)
df68 = df67.select(F.substring(F.col("firstname"), 1, 3).alias("substring"))

# Extracting strings using split from spark dataframe columns also use explode
data = [("James, Smith",), ("Ann, Hathaway",)]
columns = ["name"]
df69 = spark.createDataFrame(data, schema=columns)
df70 = df69.select(F.explode(F.split(F.col("name"), ",")).alias("name_split"))
df70.show()

# Padding characters around strings in spark dataframe columns
data = [("James",), ("Ann",)]
columns = ["name"]
df71 = spark.createDataFrame(data, schema=columns)
df72 = df71.select(F.lpad(F.col("name"), 10, "#").alias("lpad"),
                   F.rpad(F.col("name"), 10, "#").alias("rpad"))
df72.show()

spark.stop()
