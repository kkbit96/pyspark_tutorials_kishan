import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

connection_parameters = {
    "account" : "https://ekoahyx-aq66015.snowflakecomputing.com",
    "user" : "KKBIT96",
    "password" : "Kumar@96#",
    "role" : "ACCOUNTADMIN",
    "warehouse" : "COMPUTE_WH",
    "database" : "DEMO_DB",
    "schema" : "PUBLIC"
}
session = Session.builder.config(**connection_parameters).create()
df_customer_info = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
df_customer_filter = df_customer_info.filter(col("C_MKTSEGMENT") == "HOUSEHOLD")
df_customer_select = df_customer_info.select(col("C_NAME"), col("C_ADDRESS"))
df_customer_select.show()
df_customer_select.count()
df_customer_select.describe().sort("summary").show()
