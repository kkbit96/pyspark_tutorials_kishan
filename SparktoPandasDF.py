# Nested structure elements
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dataStruct = [(("James","","Smith"),"36636","M","3000"), \
      (("Michael","Rose",""),"40288","M","4000"), \
      (("Robert","","Williams"),"42114","M","4000"), \
      (("Maria","Anne","Jones"),"39192","F","4000"), \
      (("Jen","Mary","Brown"),"","F","-1") \
]

schemaStruct = StructType([
             StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
             StructField('dob', StringType(), True),
             StructField('gender', StringType(), True),
             StructField('salary', StringType(), True)
         ])
df = spark.createDataFrame(data=dataStruct, schema = schemaStruct)
from pyspark.sql.functions import col,struct,when
updatedDF = df.withColumn("OtherInfo", struct(col("dob").alias("dob"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
      .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  ))
df.printSchema()
df.show()
updatedDF.show()
pandasDF2 = df.toPandas()
print(pandasDF2)