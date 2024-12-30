
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode, col, count, when, split, regexp_replace, first, trim, lower, aggregate, sum, \
    collect_list, struct, countDistinct, max, min

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

# Count the null values in each column of the dataframe
datam = [("James", None, "M"),
        ("Michael", "Rose", "M"),
        ("Robert", "Williams", None),
        ("Maria", "Anne", "F"),
        ("Jen", None, "F"),
        ("Jenna", None, "F")]
columns = ["First_name", "Last_name", "Gender"]
df11 = spark.createDataFrame(data = datam, schema = columns)
df11.select([count(when(col(i).isNull(), i)).alias(i) for i in df11.columns]).show()

# Write code to select the rows where id is odd and the description is 'boring'
# df.filter((col("id")%2 != 0) & trim(lower((col("description") != "boring")))).show()

datao = [("john", "tomato", 2),
         ("bill", "apple", 2),
         ("john", "banana", 2),
         ("john", "tomato", 3),
         ("bill", "taco", 2),
         ("bill", "apple", 2)]

# I want a dataframe where output shoud be like following
# john {tomato: 5, banana: 2}
# bill {apple: 4, taco: 2}

# inside collect_list of agg function we can use struct to create a list of struct in a custom format.

columns = ["name", "food", "quantity"]
df12 = spark.createDataFrame(data = datao, schema = columns)
df13 = df12.groupBy("name", "food").agg(sum("quantity").alias("quantity"))
df13.groupBy("name").agg(collect_list(struct("food", "quantity")).alias("food")).show()

# Pyspark dataframe query to find all the duplicate emails in a table named person
datap = [(1, "abc@gmail.com"),
         (2, "bcd@gmail.com"),
         (3, "abc@gmail.com")]
columns = ["id", "email"]
df14 = spark.createDataFrame(data = datap, schema = columns)
df15 = df14.groupBy("email").agg(count("email").alias("count")).filter(col("count") > 1)
df15.show()

# Suppose there are two tables named Customers and Orders.
# Write a query to get the names of all customers who have never ordered anything.
datac = [(1, "James"),
         (2, "Michael"),
         (3, "Robert")]
columns = ["id", "name"]
df16 = spark.createDataFrame(data = datac, schema = columns)
datao = [(1, 1),
         (2, 2)]
columns = ["order_id", "customer_id"]
df17 = spark.createDataFrame(data = datao, schema = columns)
df18 = df16.join(df17, df16["id"] == df17["customer_id"], "left").filter(df17["customer_id"].isNull()).select("name")
df18.show()

# Pyspark query for a report that provides the customer ids which bought all the products from product table
datap = [(1, "apple"),
         (2, "banana"),
         (3, "apple"),
         (1, "banana"),
         (2, "apple")]
columns = ["customer_id", "product"]
df19 = spark.createDataFrame(data = datap, schema = columns)

dataq = [("apple",), ("banana",)]
columns = ["product"]
df20 = spark.createDataFrame(data = dataq, schema = columns)
df_customer = df19.groupBy("customer_id").agg(countDistinct("product").alias("count_product"))
df_product = df20.agg(countDistinct(col("product")).alias("count_product"))

joined_df = df_customer.join(df_product, df_customer["count_product"] == df_product["count_product"], "inner").select("customer_id")
joined_df.show()

# Get the employees, dept_id with maximum and minimum salary in each dept.
datae = [(1, "John", 1000, 1),
         (2, "Smith", 1500, 1),
         (3, "Alice", 1200, 2),
         (4, "Tom", 1300, 2),
         (5, "Jerry", 1100, 3),
         (6, "Kumar", 2000, 3)]
columnse = ["emp_id", "emp_name", "salary", "dept_id"]
df21 = spark.createDataFrame(data = datae, schema = columnse)
df22 = df21.groupBy("dept_id").agg(max("salary").alias("max_salary"), min("salary").alias("min_salary")).select("dept_id", "max_salary", "min_salary")
df22.show()

