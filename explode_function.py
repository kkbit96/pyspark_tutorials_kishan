
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode, col, count, when, split, regexp_replace, first

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

# In a csv file we are given with data which is both comma delimited and
# marks column is pipe delimited for Physics, Chemistry and maths. We need to read the
# data and convert it into a dataframe with proper schema
# df5 = (spark.read.option("header", True).option("sep", ",")
#        .option("inferSchema", True).csv("marks.csv"))
# df6 = df5.withColumn("Physics", split(col("Physics"), "\\|")[0].cast("int")) \
#     .withColumn("Chemistry", split(col("Chemistry"), "\\|")[1].cast("int")) \
#     .withColumn("Maths", split(col("Maths"), "\\|")[2].cast("int"))
# df6.show()

# Solve using REGEXP_REPLACE
df = spark.read.text('input1.txt')
df.show()
df6 = df.withColumn("new_value", regexp_replace("value", "(.*?\\-){3}", "$0,")).drop("value")
df7 = df6.withColumn("new_value_1", explode(split(col("new_value"), ","))).drop("new_value")
df8 = df7.withColumn("Id", (split(col("new_value_1"), "\\-")[0])) \
    .withColumn("Name", (split(col("new_value_1"), "\\-")[1])) \
    .withColumn("age", (split(col("new_value_1"), "\\-")[2])).drop("new_value_1")
df8.show()

# Pivot function demonstration
datan = [(1, "Gaga", "India", "2022-01-11"),
         (2, "Raga", "USA", "2022-01-12"),
         (1, "Sagar", "UK", "2022-01-13"),
         (1, "Muni", "India", "2022-01-14"),
         (2, "Raj", "USA", "2022-01-15"),
         (2, "Kunal", "UK", "2022-01-16"),
         (3, "Kunal", "UK", "2022-01-17"),
         (3, "Kunal", "UK", "2022-01-18")]
columns = ["id", "Name", "Country", "Date"]
df9 = spark.createDataFrame(data = datan, schema = columns)
df10 = df9.groupBy("id").pivot("Name").agg(first("country"))
df10.show()