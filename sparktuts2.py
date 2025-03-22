from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, Window
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
data = [("James", "Smith", "USA", "29"), ("Michael", "Rose", "UK", "22"), ("Robert", "Williams", "JAPAN", "50")]
schema = ["Firstname", "Lastname", "Country", "Age"]
df = spark.createDataFrame(data, schema)
# # df.filter("Age<50").show()
# df.where(df.Country == "USA").show()
#
# # Running SQL queries on a spark dataframe
# df.createOrReplaceTempView("df_view")
# spark.sql("""SELECT *
#  FROM df_view
#  WHERE Age < 50""") \
#  .show()
#
# # Filter using not equal condition on spark dataframe
# df.filter(df.Age != 22).show()
df.drop("Lastname").show()

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

# Consider two dataframes in Pyspark- OrderDF and orderItemDF.
# OrderDF has the following schema
# root
# |-- order_id: integer (nullable = true)
# |-- order_date: date (nullable = true)
# |-- customer_id: integer (nullable = true)
# |--order_status: string (nullable = true)

# OrderItemDF has the following schema
# root
# |-- order_item_id: integer (nullable = true)
# |-- order_id: integer (nullable = true)
# |-- product_id: integer (nullable = true)
# |-- quantity: integer (nullable = true)
# |-- unit_price : double (nullable = true)
# Write a pyspark code to find the total revenue generated by each customer, considering only the orders with complete status. Also include the customers who have
# not placed any orders with the complete status, showing their total revenue as zero.
order_data = [(1, date(2019, 1, 1), 101, "complete"),
              (2, date(2019, 1, 2), 102, "pending"),
              (3, date(2019, 1, 3), 103, "complete"),
              (4, date(2019, 1, 4), 104, "complete"),
              (5, date(2019, 1, 5), 105, "pending")]
order_schema = ["order_id", "order_date", "customer_id", "order_status"]
order_df = spark.createDataFrame(order_data, order_schema)
order_item_data = [(1, 1, 101, 2, 100),
                     (2, 1, 102, 3, 200),
                     (3, 2, 103, 4, 300),
                     (4, 3, 104, 5, 400),
                     (5, 4, 105, 6, 500)]
order_item_schema = ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]
order_item_df = spark.createDataFrame(order_item_data, order_item_schema)
order_df = order_df.filter(F.col("order_status") == "complete")
order_item_df = order_item_df.withColumn("revenue", F.col("quantity") * F.col("unit_price"))

<<<<<<< HEAD

=======
# # Workign with a dataset from an insurance company that tracks the policy changes over time.
# # The dataset contains the following columns:
# # 1. policy_id: unique identifier for each policy
# # 2. policy_holder: name of the policy holder
# # 3. policy_start_date: start date of the policy
# # 4. policy_end_date: end date of the policy
# # 5. policy_status: status of the policy (active, expired, cancelled)
# # 6. premium_amount: amount paid by the policy holder
# # 7. total_claim_amount: total claim amount filed by the policy holder
# # 8. claim_status: status of the claim (approved, rejected, pending)
# # 9. claim_date: date when the claim was filed
# # 10. claim_amount: amount claimed by the policy holder
# # 11. claim_reason: reason for the claim
# # 12. claim_description: description of the claim
# Your task is to calculate the rolling avg of the premium amount for each policy over a window of 2 days.
# Additionally you need to provide the total premium amount for each policy type. Implement thsi using pyspark \
# and window functions.
data7 = [(1, "John", date(2019, 1, 1), date(2019, 1, 10), "active", 100, 200, "approved", date(2019, 1, 5), 200, "accident", "car damage"),
         (2, "Smith", date(2019, 1, 2), date(2019, 1, 11), "expired", 150, 250, "rejected", date(2019, 1, 6), 250, "theft", "stolen car"),
         (3, "Williams", date(2019, 1, 3), date(2019, 1, 12), "cancelled", 200, 300, "pending", date(2019, 1, 7), 300, "accident", "car damage"),
         (4, "Chris", date(2019, 1, 4), date(2019, 1, 13), "active", 250, 350, "approved", date(2019, 1, 8), 350, "theft", "stolen car"),
         (5, "Mike", date(2019, 1, 5), date(2019, 1, 14), "active", 300, 400, "rejected", date(2019, 1, 9), 400, "accident", "car damage"),
         (6, "Alice", date(2019, 1, 6), date(2019, 1, 15), "expired", 350, 450, "approved", date(2019, 1, 10), 450, "theft", "stolen car"),
         (7, "Tom", date(2019, 1, 7), date(2019, 1, 16), "cancelled", 400, 500, "rejected", date(2019, 1, 11), 500, "accident", "car damage"),
         (8, "Jerry", date(2019, 1, 8), date(2019, 1, 17), "active", 450, 550, "pending", date(2019, 1, 12), 550, "theft", "stolen car"),
         (9, "Kumar", date(2019, 1, 9), date(2019, 1, 18), "active", 500, 600, "approved", date(2019, 1, 13), 600, "accident", "car damage"),
         (10, "Raj", date(2019, 1, 10), date(2019, 1, 19), "expired", 550, 650, "rejected", date(2019, 1, 14), 650, "theft", "stolen car")]

schema = ["policy_id", "policy_holder", "policy_start_date", "policy_end_date", "policy_status", "premium_amount", "total_claim_amount", "claim_status", "claim_date", "claim_amount", "claim_reason", "claim_description"]
df7 = spark.createDataFrame(data7, schema)
df7.show()
window_spec = Window.partitionBy("policy_id").orderBy("policy_start_date").rowsBetween(Window.currentRow-1, Window.currentRow+1) # Can use -1 and 0 as well
df7 = df7.withColumn("rolling_avg", F.avg("premium_amount").over(window_spec))
df7.show()
>>>>>>> e45fc57 (new commits)
