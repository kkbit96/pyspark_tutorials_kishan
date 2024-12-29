

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA")]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data,columns)
df.show()
        