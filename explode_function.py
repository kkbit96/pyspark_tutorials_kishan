
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count, when

spark = SparkSession.builder.appName('Session1').getOrCreate()
simpleData = [("James", "Sales", ["Java", "C++"]),
                ("Michael", "Sales", ["Spark", "Java"]),
                ("Robert", "Marketing", ["C++", "Spark", "Java"])]
columns = ["Name", "Dept", "Languages"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.select(col("Name"), col("Dept"), explode(col("Languages")).alias("Language")).show()

# filter out the valid phone numbers using regex
data3 = [("James", "987654321x"), ("Michael", "d9123456789"), ("Robert", "1234567890")]
columns3 = ["Name", "Phone"]
df3 = spark.createDataFrame(data = data3, schema = columns3)
df3.filter(df3.Phone.rlike("^[0-9]{10}$")).show()

# Count rows in each column where NULLs are present
data4 = [("James", None), ("Michael", "Rose"), (None, "Williams")]
columns4 = ["Name", "Lname"]
df4 = spark.createDataFrame(data = data4, schema = columns4)
df4.show()
df4.select([count(when(col(i).isNull(), i)).alias(i) for i in df4.columns]).show()