from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as F
spark = SparkSession.builder.appName('Session1').getOrCreate()
simpleData = [(1, "Sagar", "CSE", "UP", 80),
              (2,"Shivam", "IT", "MP", 86),
              (3, "Muni", "Mech", "AP", 70)]
columns = ["id", "Name", "Dept_name", "city", "Marks"]
df1 = spark.createDataFrame(data = simpleData, schema = columns)
df1.show()

simpleData2 = [(5, "Raj", "CSE", "HP"),
               (6, "Kunal", "Mech", "RJ")]
columns = ["id", "Name", "Dept_name", "city"]
df2 = spark.createDataFrame(data = simpleData2, schema = columns)
df2.show()

df2 = df2.withColumn("Marks", F.lit("null"))
df2.show()

df1.union(df2).show()