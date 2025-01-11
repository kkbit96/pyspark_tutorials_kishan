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

data1 = [(1, "John"), (2, "Smith")]
data2 = [("Alice", 3), ("Tom", 4)]

# Create DataFrames
df1 = spark.createDataFrame(data1, ["id", "name"])
df2 = spark.createDataFrame(data2, ["name", "id"])

# Using union (will cause incorrect data combination)
union_df = df1.union(df2)
union_df.show()

# Using unionByName (correctly combines data by matching column names)
union_by_name_df = df1.unionByName(df2, allowMissingColumns=True)
union_by_name_df.show()

data3 = [("Alice", "apple, orange, grapes"),
         ("Tom", "banana, apple"),
         ("John", "banana, orange"),
         ("Smith", "grapes, apple, orange")]
schema = ["name", "fruits"]
df3 = spark.createDataFrame(data3, schema)

df3 = df3.withColumn("fruits_array", F.split(df3.fruits, ",\s*"))
df3 = df3.select("name", F.explode("fruits_array").alias("fruit"))
df3.show()


# Question : Extract the name and age fields from the details struct.
# Create a new column named full_address by concatenating the city and country fields from the nested address struct.
data4 = [(1, ("John", 25, ("New York", "USA"))),
         (2, ("Smith", 22, ("California", "USA"))),
         (3, ("Williams", 23, ("Texas", "UK"))),
         (4, ("Chris", 24, ("Detroit", "AUSTRALIA")))]

nested_schema = StructType([
                    StructField("Id", IntegerType(), True),
                    StructField("Details", StructType([
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("Address", StructType([
                            StructField("City", StringType(), True),
                            StructField("Country", StringType(), True)
                        ]))
                    ]))
                    ])
df5 = spark.createDataFrame(data4, nested_schema)
df5 = df5.withColumn("name", df5["Details.Name"]).withColumn("Age", df5["Details.Age"]) \
    .withColumn("full_address", F.concat(F.col("Details.Address.City"), F.lit(", "), F.col("Details.Address.Country")))
df5.show()

# Given a pyspark dataframe with following schema
# root
# |-- employee_id: integer (nullable = true)
# |-- employee_name: string (nullable = true)
# |-- salary: double (nullable = true)
# |-- department: string (nullable = true)
# |-- hire_date: date (nullable = true)
# Perform the following operations
# 1. Identify and count the number of null values in each column
# 2. replace the null value in the salary column with the mean average of all employees
# 3. replace the null values in the department column with the default value of unknown
# 4. replace the null values in the hire_date column with the current date
data6 = [(1, "John", 1000, "HR", date(2010, 1, 1)),
         (2, "Smith", None, "Admin", None),
         (3, "Williams", 1500, None, date(2011, 1, 1)),
         (4, "Chris", 2000, "HR", None),
         (5, "Mike", 1200, "Admin", date(2012, 1, 1))]
schema = ["employee_id", "employee_name", "salary", "department", "hire_date"]
df6 = spark.createDataFrame(data6, schema)
df6.select([F.count(F.when(F.col(i).isNull(), i)).alias(i) for i in df6.columns])

mean_salary = df6.select(F.mean("salary")).collect()[0][0]
df6.withColumn("salary", F.when(F.col("salary").isNull(), mean_salary).otherwise(F.col("salary"))) \
    .withColumn("department", F.when(F.col("department").isNull(), "unknown").otherwise(F.col("department"))) \
    .withColumn("hire_date", F.when(F.col("hire_date").isNull(), date.today()).otherwise(F.col("hire_date"))).show()

# Easy way to do this
# df6.fillna({"salary": mean_salary, "department": "unknown", "hire_date": date.today()}).show()
#
# df6.na.fill({"salary": mean_salary, "department": "unknown", "hire_date": date.today()}).show()

# in the above dataframe, add a new column named bonus based on the following conditions:
# 1. If the employee is in sales and has a salary greater thn 1500, then the bonus is 10% if the salary.
# 2. If the employee is in HR and has a salary greater than 1500, Then the bonus is 15% of the salary.
# 3. For all other cases the bonus is 15% of the salary.
df6 = df6.withColumn("bonus", F.when((F.col("department") == "Sales") & (F.col("salary") > 1500), F.col("salary") * 0.10)
                     .when((F.col("department") == "HR") & (F.col("salary") > 1500), F.col("salary") * 0.15)
                     .otherwise(F.col("salary") * 0.15))



